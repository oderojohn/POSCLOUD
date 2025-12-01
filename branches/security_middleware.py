"""
Security middleware to prevent cross-branch data access.
This middleware ensures all database queries are branch-aware.
"""

from threading import local
from django.db import connection
from branches.middleware import BranchContextMiddleware

# Thread-local storage for branch enforcement
_branch_enforcement = local()

class BranchSecurityMiddleware:
    """
    Middleware that enforces branch security at the database level.
    This provides an additional layer of protection against cross-branch data leakage.
    """
    
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Set branch enforcement context
        current_branch = BranchContextMiddleware.get_current_branch()
        _branch_enforcement.branch = current_branch
        
        # Add branch filtering to ORM queries
        from django.db import models
        
        # Monkey patch QuerySet to enforce branch filtering
        original_filter = models.QuerySet.filter
        original_all = models.QuerySet.all
        
        def branch_aware_filter(self, *args, **kwargs):
            """Filter that enforces branch security"""
            result = original_filter(self, *args, **kwargs)
            
            # Check if this query needs branch filtering
            model = result.model
            has_branch_field = any(field.name == 'branch' for field in model._meta.get_fields())
            
            if has_branch_field and _branch_enforcement.branch:
                # Apply branch filter if not already applied
                if 'branch' not in kwargs:
                    result = result.filter(branch=_branch_enforcement.branch)
            
            return result
        
        def branch_aware_all(self):
            """All that enforces branch security"""
            result = original_all(self)
            
            # Check if this query needs branch filtering
            model = result.model
            has_branch_field = any(field.name == 'branch' for field in model._meta.get_fields())
            
            if has_branch_field and _branch_enforcement.branch:
                # Apply branch filter
                result = result.filter(branch=_branch_enforcement.branch)
            
            return result
        
        # Apply monkey patches
        models.QuerySet.filter = branch_aware_filter
        models.QuerySet.all = branch_aware_all
        
        try:
            response = self.get_response(request)
        finally:
            # Restore original methods
            models.QuerySet.filter = original_filter
            models.QuerySet.all = original_all
            
            # Clean up thread-local storage
            if hasattr(_branch_enforcement, 'branch'):
                del _branch_enforcement.branch
        
        return response
