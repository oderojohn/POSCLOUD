from rest_framework import viewsets, status
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.permissions import AllowAny, IsAdminUser
from django.contrib.auth import logout

from .models import UserProfile
from .serializers import UserProfileSerializer


class UserProfileViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing user profiles.
    Restricted to admin users only.
    """
    queryset = UserProfile.objects.all()
    serializer_class = UserProfileSerializer
    permission_classes = [IsAdminUser]


class LoginView(TokenObtainPairView):
    """
    Issues JWT access + refresh tokens on valid credentials.
    """
    permission_classes = [AllowAny]
    authentication_classes = []

    def post(self, request, *args, **kwargs):
        response = super().post(request, *args, **kwargs)
        if response.status_code == 200:
            # Get the user from the token
            token = response.data.get('access')
            if token:
                from rest_framework_simplejwt.tokens import AccessToken
                access_token = AccessToken(token)
                user_id = access_token['user_id']
                from django.contrib.auth.models import User
                try:
                    user = User.objects.get(id=user_id)
                    groups = list(user.groups.values_list('name', flat=True))
                    response.data['roles'] = groups
                except User.DoesNotExist:
                    response.data['roles'] = []
        return response


class RefreshView(TokenRefreshView):
    """
    Issues a new access token given a valid refresh token.
    """
    permission_classes = [AllowAny]
    authentication_classes = []


class LogoutView(APIView):
    """
    Blacklists the given refresh token to effectively log the user out.
    """
    permission_classes = [AllowAny]
    authentication_classes = []

    def post(self, request):
        try:
            refresh_token = request.data.get("refresh")
            if not refresh_token:
                return Response(
                    {"error": "Refresh token required"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            token = RefreshToken(refresh_token)
            token.blacklist()

            # Also clear session if applicable
            logout(request)

            return Response(
                {"message": "Successfully logged out"},
                status=status.HTTP_200_OK,
            )
        except Exception:
            return Response(
                {"error": "Invalid token"},
                status=status.HTTP_400_BAD_REQUEST,
            )


class RoleListView(APIView):
    """
    Returns available roles defined in UserProfile.ROLE_CHOICES.
    """
    permission_classes = [AllowAny]
    authentication_classes = []

    def get(self, request):
        roles = [
            {"value": choice[0], "label": choice[1]}
            for choice in UserProfile.ROLE_CHOICES
        ]
        return Response(roles)

    def post(self, request):
        # Adding new roles is not supported (roles are defined as choices).
        return Response(status=status.HTTP_405_METHOD_NOT_ALLOWED)
