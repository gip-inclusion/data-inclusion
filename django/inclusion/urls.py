from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView

from django.urls import include, path

from inclusion.api.router import router

urlpatterns = [
    path("api/v0/", include((router.urls, "inclusion"), namespace="v0")),
    # docs
    path("api/v0/schema/", SpectacularAPIView.as_view(api_version="v0"), name="schema"),
    path("api/v0/docs/", SpectacularSwaggerView.as_view(url_name="schema"), name="docs"),
]
