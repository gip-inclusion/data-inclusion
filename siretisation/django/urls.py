from django.contrib import admin
from django.urls import include, path

import annotation.views
import matching.views

urlpatterns = [
    path("admin/", admin.site.urls),
    path("accounts/", include("django.contrib.auth.urls")),
    path("", include("meta.urls")),
    path("", annotation.views.index, name="index"),
    path("partials/search", annotation.views.partial_search),
    path("partials/task", annotation.views.partial_task),
    path("partials/submit", annotation.views.partial_submit),
    path("matching", matching.views.index, name="matching"),
    path("search", matching.views.search_view),
    path("partials/matching", matching.views.partial_matching),
]