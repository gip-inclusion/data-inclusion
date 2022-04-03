from rest_framework import routers

from inclusion.api import views

router = routers.DefaultRouter()

router.register(r"structures", views.StructureViewSet, basename="structures")
router.register(r"reports", views.StructureReportViewSet, basename="reports")
