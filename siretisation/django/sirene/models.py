import enum

from django.db import models


class CodeNAF(models.Model):
    class Level(enum.Enum):
        SECTION = "section"
        DIVISION = "division"
        GROUP = "group"
        CLASS = "class"
        SUBCLASS = "subclass"

    code = models.TextField(max_length=6, primary_key=True)
    label = models.TextField()

    section_code = models.TextField()
    section_label = models.TextField()
    division_code = models.TextField()
    division_label = models.TextField()
    group_code = models.TextField()
    group_label = models.TextField()
    class_code = models.TextField()
    class_label = models.TextField()
