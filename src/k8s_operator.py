#!/usr/bin/env python3
"""
BlackRoad Kubernetes Operator
Production-quality Kubernetes operator with reconciliation loop.
Manages custom resources for BlackRoad deployments and services.
"""

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass, asdict
from enum import Enum
import threading
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ResourcePhase(Enum):
    """Resource lifecycle phases."""
    PENDING = "Pending"
    CREATING = "Creating"
    RUNNING = "Running"
    UPDATING = "Updating"
    DELETING = "Deleting"
    FAILED = "Failed"
    SUCCEEDED = "Succeeded"


class ConditionType(Enum):
    """Condition types for resources."""
    READY = "Ready"
    PROGRESSING = "Progressing"
    AVAILABLE = "Available"
    ERROR = "Error"


@dataclass
class Condition:
    """Resource condition."""
    type: str
    status: str  # True, False, Unknown
    reason: str
    message: str
    last_transition_time: str
    observed_generation: int = 0


@dataclass
class ObjectReference:
    """Reference to Kubernetes object."""
    kind: str
    name: str
    namespace: str
    uid: str
    api_version: str


class FinalizerManager:
    """Manages finalizers for clean resource deletion."""
    
    FINALIZER_PREFIX = "blackroad.cloud/operator-"
    
    @classmethod
    def add_finalizer(cls, resource: Dict[str, Any], name: str) -> bool:
        """Add finalizer to resource."""
        finalizer = f"{cls.FINALIZER_PREFIX}{name}"
        if "metadata" not in resource:
            resource["metadata"] = {}
        if "finalizers" not in resource["metadata"]:
            resource["metadata"]["finalizers"] = []
        
        if finalizer not in resource["metadata"]["finalizers"]:
            resource["metadata"]["finalizers"].append(finalizer)
            return True
        return False
    
    @classmethod
    def remove_finalizer(cls, resource: Dict[str, Any], name: str) -> bool:
        """Remove finalizer from resource."""
        finalizer = f"{cls.FINALIZER_PREFIX}{name}"
        if "metadata" not in resource or "finalizers" not in resource["metadata"]:
            return False
        
        if finalizer in resource["metadata"]["finalizers"]:
            resource["metadata"]["finalizers"].remove(finalizer)
            return True
        return False
    
    @classmethod
    def has_finalizer(cls, resource: Dict[str, Any], name: str) -> bool:
        """Check if resource has finalizer."""
        finalizer = f"{cls.FINALIZER_PREFIX}{name}"
        if "metadata" not in resource or "finalizers" not in resource["metadata"]:
            return False
        return finalizer in resource["metadata"]["finalizers"]


class StatusManager:
    """Manages resource status subresource."""
    
    @staticmethod
    def initialize_status(resource: Dict[str, Any]) -> Dict[str, Any]:
        """Initialize resource status."""
        if "status" not in resource:
            resource["status"] = {
                "phase": ResourcePhase.PENDING.value,
                "conditions": [],
                "observed_generation": 0,
                "last_update_time": datetime.utcnow().isoformat(),
            }
        return resource["status"]
    
    @staticmethod
    def add_condition(resource: Dict[str, Any], condition: Condition):
        """Add condition to resource status."""
        status = StatusManager.initialize_status(resource)
        
        # Remove existing condition of same type
        status["conditions"] = [
            c for c in status["conditions"]
            if c.get("type") != condition.type
        ]
        
        status["conditions"].append(asdict(condition))
        status["last_update_time"] = datetime.utcnow().isoformat()
        logger.info(f"Added condition {condition.type}={condition.status} "
                   f"to {resource['metadata']['name']}: {condition.message}")
    
    @staticmethod
    def set_phase(resource: Dict[str, Any], phase: ResourcePhase):
        """Set resource phase."""
        status = StatusManager.initialize_status(resource)
        status["phase"] = phase.value
        status["last_update_time"] = datetime.utcnow().isoformat()


class ReconciliationRequest:
    """Request for resource reconciliation."""
    
    def __init__(self, name: str, namespace: str, kind: str):
        self.name = name
        self.namespace = namespace
        self.kind = kind
        self.enqueue_time = time.time()
        self.attempts = 0
        self.max_retries = 3
    
    def can_retry(self) -> bool:
        """Check if request can be retried."""
        return self.attempts < self.max_retries
    
    def increment_attempts(self):
        """Increment attempt counter."""
        self.attempts += 1


class WorkQueue:
    """Work queue for reconciliation requests."""
    
    def __init__(self):
        self.queue: List[ReconciliationRequest] = []
        self.processing: Set[str] = set()
        self.lock = threading.RLock()
    
    def add(self, request: ReconciliationRequest):
        """Add request to queue."""
        with self.lock:
            key = f"{request.namespace}/{request.name}"
            if key not in self.processing:
                self.queue.append(request)
    
    def get(self) -> Optional[ReconciliationRequest]:
        """Get next request from queue."""
        with self.lock:
            if self.queue:
                return self.queue.pop(0)
            return None
    
    def mark_done(self, request: ReconciliationRequest, success: bool = True):
        """Mark request as processed."""
        with self.lock:
            key = f"{request.namespace}/{request.name}"
            if key in self.processing:
                self.processing.discard(key)
            
            if not success and request.can_retry():
                request.increment_attempts()
                self.queue.append(request)
    
    def size(self) -> int:
        """Get queue size."""
        with self.lock:
            return len(self.queue)


class OwnerReferencesManager:
    """Manages owner references for garbage collection."""
    
    @staticmethod
    def add_owner_reference(resource: Dict[str, Any], 
                           owner: ObjectReference) -> bool:
        """Add owner reference to resource."""
        if "metadata" not in resource:
            resource["metadata"] = {}
        if "ownerReferences" not in resource["metadata"]:
            resource["metadata"]["ownerReferences"] = []
        
        ref = {
            "apiVersion": owner.api_version,
            "kind": owner.kind,
            "name": owner.name,
            "uid": owner.uid,
            "controller": True,
            "blockOwnerDeletion": True,
        }
        
        # Check if already exists
        for existing in resource["metadata"]["ownerReferences"]:
            if existing["uid"] == owner.uid:
                return False
        
        resource["metadata"]["ownerReferences"].append(ref)
        return True


class KubernetesOperator:
    """Base Kubernetes operator implementation."""
    
    def __init__(self, operator_name: str, crd_kind: str, 
                 crd_group: str = "blackroad.cloud"):
        self.operator_name = operator_name
        self.crd_kind = crd_kind
        self.crd_group = crd_group
        self.crd_version = "v1alpha1"
        self.work_queue = WorkQueue()
        self.resources: Dict[str, Dict[str, Any]] = {}
        self.running = False
        self.reconciliation_count = 0
        self.reconciliation_errors = 0
        self.lock = threading.RLock()
    
    def add_resource(self, resource: Dict[str, Any]):
        """Add resource to operator."""
        with self.lock:
            key = f"{resource['metadata']['namespace']}/{resource['metadata']['name']}"
            self.resources[key] = resource
            
            # Initialize status
            StatusManager.initialize_status(resource)
            FinalizerManager.add_finalizer(resource, self.operator_name)
            
            # Enqueue reconciliation
            request = ReconciliationRequest(
                resource['metadata']['name'],
                resource['metadata']['namespace'],
                resource['kind']
            )
            self.work_queue.add(request)
            logger.info(f"Added resource {key}")
    
    def reconcile(self, request: ReconciliationRequest) -> bool:
        """Reconcile resource to desired state."""
        try:
            key = f"{request.namespace}/{request.name}"
            resource = self.resources.get(key)
            
            if not resource:
                logger.warning(f"Resource {key} not found")
                return False
            
            # Check if resource is marked for deletion
            if "metadata" in resource and "deletionTimestamp" in resource["metadata"]:
                return self.cleanup_resource(resource)
            
            # Perform reconciliation based on resource type
            logger.info(f"Reconciling {request.kind}/{key}")
            
            # Set progressing condition
            condition = Condition(
                type=ConditionType.PROGRESSING.value,
                status="True",
                reason="Reconciling",
                message="Starting reconciliation loop",
                last_transition_time=datetime.utcnow().isoformat(),
            )
            StatusManager.add_condition(resource, condition)
            StatusManager.set_phase(resource, ResourcePhase.UPDATING)
            
            # Simulate reconciliation work
            if not self.handle_resource(resource):
                raise Exception("Resource handling failed")
            
            # Set ready condition
            ready_condition = Condition(
                type=ConditionType.READY.value,
                status="True",
                reason="Reconciled",
                message="Resource successfully reconciled",
                last_transition_time=datetime.utcnow().isoformat(),
            )
            StatusManager.add_condition(resource, ready_condition)
            StatusManager.set_phase(resource, ResourcePhase.RUNNING)
            
            with self.lock:
                self.reconciliation_count += 1
            
            logger.info(f"Successfully reconciled {key}")
            return True
        
        except Exception as e:
            logger.error(f"Reconciliation failed for {request.name}: {e}")
            
            error_condition = Condition(
                type=ConditionType.ERROR.value,
                status="True",
                reason="ReconciliationFailed",
                message=str(e),
                last_transition_time=datetime.utcnow().isoformat(),
            )
            if key in self.resources:
                StatusManager.add_condition(self.resources[key], error_condition)
                StatusManager.set_phase(self.resources[key], ResourcePhase.FAILED)
            
            with self.lock:
                self.reconciliation_errors += 1
            
            return False
    
    def handle_resource(self, resource: Dict[str, Any]) -> bool:
        """Handle specific resource type. Override in subclass."""
        logger.info(f"Handling {resource['kind']}/{resource['metadata']['name']}")
        return True
    
    def cleanup_resource(self, resource: Dict[str, Any]) -> bool:
        """Clean up resource on deletion."""
        try:
            logger.info(f"Cleaning up {resource['kind']}/{resource['metadata']['name']}")
            FinalizerManager.remove_finalizer(resource, self.operator_name)
            
            key = f"{resource['metadata']['namespace']}/{resource['metadata']['name']}"
            with self.lock:
                if key in self.resources:
                    del self.resources[key]
            
            return True
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
            return False
    
    def run_reconciliation_loop(self):
        """Main reconciliation loop."""
        logger.info(f"Starting {self.operator_name} reconciliation loop")
        self.running = True
        
        while self.running:
            try:
                request = self.work_queue.get()
                if request:
                    success = self.reconcile(request)
                    self.work_queue.mark_done(request, success)
                else:
                    time.sleep(0.1)  # Avoid busy waiting
            except Exception as e:
                logger.error(f"Error in reconciliation loop: {e}")
                time.sleep(1)
    
    def stop(self):
        """Stop the operator."""
        self.running = False
        logger.info(f"Stopped {self.operator_name}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get operator status."""
        with self.lock:
            return {
                "operator": self.operator_name,
                "crd": f"{self.crd_kind}.{self.crd_group}",
                "running": self.running,
                "managed_resources": len(self.resources),
                "work_queue_size": self.work_queue.size(),
                "reconciliations": self.reconciliation_count,
                "errors": self.reconciliation_errors,
                "timestamp": datetime.utcnow().isoformat(),
            }


def main():
    """Main entry point."""
    operator = KubernetesOperator("blackroad-operator", "Service")
    
    # Create sample resource
    sample_resource = {
        "apiVersion": "blackroad.cloud/v1alpha1",
        "kind": "Service",
        "metadata": {
            "name": "example-service",
            "namespace": "default",
        },
        "spec": {
            "replicas": 3,
            "image": "example:latest",
        }
    }
    
    operator.add_resource(sample_resource)
    
    # Run single reconciliation
    request = ReconciliationRequest("example-service", "default", "Service")
    success = operator.reconcile(request)
    
    # Print status
    status = operator.get_status()
    print(json.dumps(status, indent=2))
    logger.info(f"Reconciliation {'succeeded' if success else 'failed'}")


if __name__ == "__main__":
    main()
