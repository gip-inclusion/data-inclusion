from django.urls import path

from meta.api import views

urlpatterns = [
    path("api/version/", views.VersionEndpoint.as_view()),
]
