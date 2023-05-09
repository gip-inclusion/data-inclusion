import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


class AnnuaireEntreprisesClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def search(
        self,
        *,
        q: str,
        activite_principale: Optional[list[str]] = None,
        section_activite_principale: Optional[list[str]] = None,
        code_commune: Optional[str] = None,
        departement: Optional[str] = None,
        est_entrepreneur_individuel: Optional[bool] = None,
        est_association: Optional[bool] = None,
    ) -> dict:
        params = {"q": q}

        if isinstance(activite_principale, list) and len(activite_principale) > 0:
            params["activite_principale"] = ",".join(activite_principale)

        if isinstance(section_activite_principale, list) and len(section_activite_principale) > 0:
            params["section_activite_principale"] = ",".join(section_activite_principale)

        if code_commune is not None:
            params["code_commune"] = code_commune

        if departement is not None:
            params["departement"] = departement

        if est_entrepreneur_individuel is not None:
            params["est_entrepreneur_individuel"] = str(est_entrepreneur_individuel)

        if est_association is not None:
            params["est_association"] = str(est_association)

        with httpx.Client(
            base_url=self.base_url,
            params={"include_score": True, "per_page": 20},
            event_hooks={
                "request": [logger.debug],
            },
        ) as client:
            response = client.get(
                "/search",
                params=params,
            )

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(exc)
            return {"results": []}

        return response.json()
