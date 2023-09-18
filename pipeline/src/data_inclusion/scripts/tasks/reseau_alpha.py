import io
import json
import re

import scrapy
import trafilatura
from scrapyscript import Job, Processor

# URL EXAMPLES
# Structure with lieux and service details :
# https://www.reseau-alpha.org/structure/apprentissage-du-francais/aries
# Structure without lieu and without service :
# https://www.reseau-alpha.org/structure/apprentissage-du-francais/acafi
# A service :
# https://www.reseau-alpha.org/structure/apprentissage-du-francais/aries/formation/
# francais-a-visee-professionnelle/b8a73-francais-a-visee-sociale-et-ou-professionnelle


def clean_adresse(adresses: list or scrapy.Selector) -> {} or []:
    lieux = []
    for adresse in adresses:
        adresse_text_chunks = adresse.xpath("text()").getall()
        clean_lieu = {
            "structure_service_adresse_entiere": "",
            "structure_service_adresse": "",
            "structure_service_code_postal": "",
            "structure_service_commune": "",
        }
        for part in adresse_text_chunks:
            part = part.strip()
            if re.match(r"^\d", part):
                if re.match(r"^\d{5}", part):
                    split_address = part.split(" - ")
                    clean_lieu["structure_service_code_postal"] = split_address[0]
                    clean_lieu["structure_service_commune"] = split_address[1]
                else:
                    clean_lieu["structure_service_adresse"] = part
            clean_lieu["structure_service_adresse_entiere"] += part + ", "
        lieux.append(clean_lieu)
    return lieux


def strip(maybe_string):
    if type(maybe_string) == str:
        return maybe_string.strip()
    if maybe_string is None:
        return ""
    else:
        return maybe_string


def html_to_markdown(s: str):
    if s is None or s == "":
        return s
    if type(s) == list:
        s = "<br/>".join(s)
    return trafilatura.extract(
        trafilatura.load_html("<html>" + s + "</html>"),
        no_fallback=True,
        max_tree_size=1000,
    )


class AlphaSpider(scrapy.Spider):
    name = "alpha"
    custom_settings = {"DOWNLOAD_DELAY": 0.5}

    def __init__(self, url=None, **kwargs):
        self.url = url
        super().__init__(url, **kwargs)

    # How self.URL is initiated:
    # https://stackoverflow.com/a/41123138
    def start_requests(self):
        urls = [self.url]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        formations_links = response.css(
            "div#div-accordion-formation > div.contact-content a.readon"
        )

        for a in formations_links:
            yield response.follow(a, callback=self.parse_formation)

    def parse_formation(self, response):
        formation_entete = response.css("div.entete")
        formation_contenu_col1 = response.css("div.entete + div > div:nth-child(1)")
        formation_contenu_col2 = response.css("div.entete + div > div:nth-child(2)")
        formation_inscription_contact = formation_contenu_col2.css("div:nth-of-type(2)")
        formation_informations_pratiques = formation_contenu_col2.css(
            "div:nth-of-type(3)"
        )
        formation_lieux_horaires = response.css("div#lieux-formation")

        # SERVICE
        item = {}

        # Nom
        item["service_nom_1"] = response.css("div.titre-element > strong::text").get()
        item["service_nom_2"] = response.css("a.underline.red-alpha + div::text").get()

        # Date de màj
        item["date_maj"] = response.css("a.underline.red-alpha + div + div::text").get()

        # Description
        contenu_objectif_public = formation_contenu_col1.css(".row div").getall()
        contenu_objectif_public += formation_informations_pratiques.get()
        # Service descriptions are very long and trafilatura doesn't
        # manage to properly cleanup the HTML markup. The result has
        # too many line breaks.
        # item["presentation_detail"] = html_to_markdown(contenu_objectif_public)

        # URL to the source page
        item["lien_source"] = response.url

        # Courriel
        item["courriel"] = formation_inscription_contact.css(
            "div.email.red-alpha > a::attr(href)"
        ).get()

        # Adresse
        clean_lieux = clean_adresse(formation_lieux_horaires.css("div.adresse"))

        # STRUCTURE
        # Structure URL
        structure_link_element = formation_entete.css(
            "div.titre-element ~ a.underline.red-alpha"
        )
        item["structure_url"] = structure_link_element.xpath("@href").get()

        for lieu in clean_lieux:
            yield scrapy.Request(
                item["structure_url"],
                callback=self.parse_structure,
                meta={"item": item},
                dont_filter=True,
            )

    def parse_structure(self, response):
        item = response.meta.get("item")

        # Nom
        item["structure_nom"] = response.css("div#structure > strong::text").get()

        # Data màj
        item["structure_date_maj"] = (
            response.css("div.structures-dates > div:nth-child(2)")
            .xpath("text()")
            .get()
        )

        # Adresse
        # A structure has as many addresses as the number of lieux
        # for its services
        # Certain services are offered on all those addresses,
        # certain aren't

        # Téléphone
        item["structure_telephone"] = response.css(
            "div.lieu div.telephone > a::attr(href)"
        ).get()

        # Site Web
        item["structure_site_web"] = strip(
            response.css("div.lieu div.facebook::text").get()
        )

        # Lien source
        item["structure_lien_source"] = response.url

        return item


processor = Processor(settings=None)


def extract(url: str, **kwargs):
    job = Job(AlphaSpider, url=url)
    data = processor.run(job)
    with io.StringIO() as buf:
        json.dump(data, buf)
        return buf.getvalue().encode()
