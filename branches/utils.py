"""
Branch filtering utilities for Django models and queries.

This module provides utilities to automatically filter queries by the current branch
context, ensuring proper data isolation between branches.
"""

from django.db.models import QuerySet, Q
from .middleware import BranchContextMiddleware


class BranchQuerySet(QuerySet):
    """
    Custom QuerySet that automatically filters by branch context.
    """
    
    def __init__(self, model=None, query=None, using=None, hints=None):
        super().__init__(model, query, using, hints)
        self._branch_filtered = False
    
    def _clone(self):
        clone = super()._clone()
        clone._branch_filtered = getattr(self, '_branch_filtered', False)
        return clone
    
    def filter_by_branch(self, branch=None, exclude_null=False):
        """
        Filter the queryset by the specified branch or current branch context.
        
        Args:
            branch: Branch instance to filter by. If None, uses current branch context.
            exclude_null: If True, excludes records without branch assignment.
        """
        if branch is None:
            branch = BranchContextMiddleware.get_current_branch()
        
        if branch is None:
            # If no branch context, return as-is or filter out nulls
            if exclude_null:
                return self.filter(branch__isnull=False)
            return self
        
        # Check if the model has a branch field
        model_branch_field = None
        for field in self.model._meta.get_fields():
            if field.name == 'branch' and hasattr(field, 'related_model'):
                if field.related_model.__name__ == 'Branch':
                    model_branch_field = 'branch'
                    break
        
        if model_branch_field:
            if exclude_null:
                return self.filter(**{model_branch_field: branch})
            return self.filter(**{model_branch_field: branch})
        
        return self
    
    def get_queryset(self):
        """
        Override get_queryset to automatically apply branch filtering.
        """
        qs = super().get_queryset()
        
        # Only apply branch filtering if not already applied and model has branch field
        if not getattr(self, '_branch_filtered', False):
            for field in self.model._meta.get_fields():
                if field.name == 'branch' and hasattr(field, 'related_model'):
                    if field.related_model.__name__ == 'Branch':
                        current_branch = BranchContextMiddleware.get_current_branch()
                        if current_branch:
                            qs = qs.filter(branch=current_branch)
                            self._branch_filtered = True
                        break
        
        return qs


def get_branch_filtered_queryset(model_class):
    """
    Get a branch-filtered queryset for the given model class.
    
    Args:
        model_class: Django model class
        
    Returns:
        BranchQuerySet instance filtered by current branch context
    """
    return BranchQuerySet(model_class)


def filter_by_current_branch(obj_or_qs):
    """
    Filter an object or queryset by the current branch context.
    
    Args:
        obj_or_qs: Django model instance or QuerySet
        
    Returns:
        Filtered object or queryset
    """
    if hasattr(obj_or_qs, 'filter'):
        # It's a queryset
        return get_branch_filtered_queryset(obj_or_qs.model).filter_by_branch()
    else:
        # It's a single object
        current_branch = BranchContextMiddleware.get_current_branch()
        if current_branch and hasattr(obj_or_qs, 'branch'):
            return obj_or_qs if obj_or_qs.branch == current_branch else None
        return obj_or_qs