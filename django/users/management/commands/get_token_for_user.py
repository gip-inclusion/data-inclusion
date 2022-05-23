import logging

from rest_framework_simplejwt.tokens import RefreshToken

from django.core.management.base import BaseCommand, CommandParser

from users.models import User

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class Command(BaseCommand):
    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("username", type=str)

    def handle(self, *args, **options):
        user = User.objects.get(username=options["username"])

        tokens = RefreshToken.for_user(user)

        logger.info(f"Token created for user {user}")
        logger.info(tokens.access_token)
