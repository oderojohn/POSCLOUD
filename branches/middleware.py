"""
Middleware to safely manage branch context using thread-local storage.
Ensures ONLY saved Branch instances are ever injected into filters.
"""

from threading import local

_branch_context = local()
_user_context = local()


class BranchContextMiddleware:

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):

        try:
            # Set current user
            if hasattr(request, "user") and request.user.is_authenticated:
                self._set_user(request.user)
                print(f"[USER] Set current user: {request.user.username}")

            # -------------------------------------------------------------
            # 1. Try getting branch from JWT (SimpleJWT)
            # -------------------------------------------------------------
            auth_header = request.headers.get('Authorization')
            if auth_header and auth_header.startswith('Bearer '):
                try:
                    token = auth_header.split(" ")[1]
                    from rest_framework_simplejwt.tokens import AccessToken
                    access = AccessToken(token)
                    branch_id = access.get("branch_id")

                    if branch_id:
                        from branches.models import Branch
                        branch = Branch.objects.get(id=branch_id, is_active=True)
                        self._set_branch(branch)
                        print(f"[BRANCH] Using branch from JWT: {branch.id} - {branch.name}")

                except Exception as e:
                    print(f"[BRANCH] Failed to load branch from JWT: {e}")

            # -------------------------------------------------------------
            # 2. Try Header X-Branch-Id
            # -------------------------------------------------------------
            if not self._has_branch():
                try:
                    branch_id = request.headers.get("X-Branch-Id")
                    if branch_id:
                        from branches.models import Branch
                        branch = Branch.objects.get(id=int(branch_id), is_active=True)
                        self._set_branch(branch)
                        print(f"[BRANCH] Using branch from Header: {branch.id}")
                except Exception:
                    pass

            # -------------------------------------------------------------
            # 3. Try from authenticated user's profile
            # -------------------------------------------------------------
            if not self._has_branch():
                if hasattr(request, "user") and request.user.is_authenticated:
                    try:
                        from users.models import UserProfile
                        profile = UserProfile.objects.get(user=request.user)

                        if profile.branch and profile.branch.is_active:
                            self._set_branch(profile.branch)
                            print(f"[BRANCH] Using branch from user profile: {profile.branch.id}")

                    except Exception:
                        pass

            # -------------------------------------------------------------
            # 4. Fallback: First active branch
            # -------------------------------------------------------------
            if not self._has_branch():
                try:
                    from branches.models import Branch
                    branch = Branch.objects.filter(is_active=True).first()
                    if branch:
                        self._set_branch(branch)
                        print(f"[BRANCH] Using default branch: {branch.id}")
                except Exception:
                    pass

        except Exception as e:
            print(f"[BRANCH] Middleware error: {e}")

        response = self.get_response(request)

        # Cleanup per-request
        self._clear_branch()
        self._clear_user()

        return response

    # ------------------------------------------------------------------
    # Thread-Local Access Methods
    # ------------------------------------------------------------------
    @classmethod
    def get_current_branch(cls):
        return getattr(_branch_context, "branch", None)

    @classmethod
    def _set_branch(cls, branch):
        if branch is None:
            return

        # 🔥 CRITICAL FIX — Ensure branch is a SAVED instance
        if not getattr(branch, "pk", None):
            raise ValueError("Cannot set unsaved Branch instance in context")

        _branch_context.branch = branch

    @classmethod
    def _has_branch(cls):
        return hasattr(_branch_context, "branch")

    @classmethod
    def _clear_branch(cls):
        if hasattr(_branch_context, "branch"):
            delattr(_branch_context, "branch")

    @classmethod
    def get_current_user(cls):
        return getattr(_user_context, "user", None)

    @classmethod
    def _set_user(cls, user):
        _user_context.user = user

    @classmethod
    def _clear_user(cls):
        if hasattr(_user_context, "user"):
            delattr(_user_context, "user")
