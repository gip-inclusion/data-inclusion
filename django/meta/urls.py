from django.urls import include, path

from meta.api import views
from meta.api.router import router

urlpatterns = [
    path("api/", include(router.urls)),
    path("api/version/", views.VersionEndpoint.as_view()),
]
