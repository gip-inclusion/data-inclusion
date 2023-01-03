from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin

from users import forms, models


@admin.register(models.User)
class UserAdmin(BaseUserAdmin):
    add_form = forms.UserCreationForm
    form = forms.UserChangeForm
    model = models.User
    list_display = [
        "email",
        "is_staff",
        "is_active",
    ]
    list_filter = [
        "email",
        "is_staff",
        "is_active",
    ]
    fieldsets = [
        (None, {"fields": ("email", "password")}),
        ("Permissions", {"fields": ("is_staff", "is_active", "groups")}),
    ]
    add_fieldsets = [
        (
            None,
            {
                "classes": ("wide",),
                "fields": (
                    "email",
                    "password1",
                    "password2",
                    "is_staff",
                    "is_active",
                    "groups",
                ),
            },
        ),
    ]
    search_fields = [
        "email",
    ]
    ordering = [
        "email",
    ]
