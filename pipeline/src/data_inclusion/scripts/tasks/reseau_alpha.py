import io
import json
import re

import dateparser
import scrapy
import trafilatura
from scrapy.crawler import CrawlerProcess
from scrapyscript import Job, Processor

# Local HTML
# base_path =  'file://' + os.path.abspath('')
# structure_base_path = base_path + '/structures'
# formation_base_path = base_path + '/services'


# Live HTML (don't use too much to avoid being banned!)
# structure_base_url =
# 'https://www.reseau-alpha.org/structure/apprentissage-du-francais/'


# Structure avec antennes et formations :
# https://www.reseau-alpha.org/structure/apprentissage-du-francais/aries
# Structure sans antenne et sans formation :
# https://www.reseau-alpha.org/structure/apprentissage-du-francais/acafi
# Formation :
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
        # formation_contenu = response.css("div.entete + div")
        formation_contenu_col1 = response.css("div.entete + div > div:nth-child(1)")
        formation_contenu_col2 = response.css("div.entete + div > div:nth-child(2)")
        # formation_inscription_info = formation_contenu_col2.css("div:nth-of-type(1)")
        formation_inscription_contact = formation_contenu_col2.css("div:nth-of-type(2)")
        formation_informations_pratiques = formation_contenu_col2.css(
            "div:nth-of-type(3)"
        )
        formation_lieux_horaires = response.css("div#lieux-formation")

        # SERVICE
        item = {}

        # Nom
        service_nom_1 = strip(response.css("div.titre-element > strong::text").get())
        service_nom_2 = strip(response.css("a.underline.red-alpha + div::text").get())
        item["nom"] = f"{service_nom_1} ({service_nom_2})"

        # Date de màj
        date_maj_fr = strip(
            response.css("a.underline.red-alpha + div + div::text").get().split(":")[-1]
        )
        item["date_maj"] = dateparser.parse(date_maj_fr).isoformat()

        # Description
        contenu_objectif_public = formation_contenu_col1.css(".row div").getall()
        contenu_objectif_public += formation_informations_pratiques.get()
        # les descriptions sont très longues et rendent difficiles
        # le test des autres champs
        # item["presentation_detail"] = html_to_markdown(contenu_objectif_public)

        # Lien vers la source
        item["lien_source"] = response.url

        # Courriel
        item["courriel"] = strip(
            formation_inscription_contact.css(
                "div.email.red-alpha > a::attr(href)"
            ).get()
        ).split(":")[-1]

        # Adresse
        clean_lieux = clean_adresse(formation_lieux_horaires.css("div.adresse"))

        # Téléphone
        item["telephone"] = ""

        # Contact nom prénom
        item["contact_nom_prenom"] = ""

        # Thématiques
        item["thematiques"] = ["apprendre-francais--suivre-formation"]
        if service_nom_2 == "Français à visée professionnelle":
            item["thematiques"].append(
                "apprendre-francais--accompagnement-insertion-pro"
            )
        if service_nom_2 == "Français à visée sociale et communicative":
            item["thematiques"].append(
                "apprendre-francais--communiquer-vie-tous-les-jours"
            )

        # Hard coded fields
        item["zone_diffusion_type"] = "departement"
        item["zone_diffusion_code"] = "91"
        item["zone_diffusion_nom"] = "Essonne"
        item["types"] = ["formation"]
        item["cumulable"] = True
        item["contact_public"] = True
        item["modes_accueil"] = ["en-presentiel"]

        # STRUCTURE
        # ID de la structure
        structure_link_element = formation_entete.css(
            "div.titre-element ~ a.underline.red-alpha"
        )
        item["structure_id"] = (
            structure_link_element.xpath("@href").get().split("/")[-1]
        )
        structure_link = structure_link_element.xpath("@href").get()

        # Une ligne/record de service et une structure par lieu
        service_id_suffix = 1
        for lieu in clean_lieux:
            # Id
            item["id"] = f"{response.url.split('/')[-1]}_{str(service_id_suffix)}"
            service_id_suffix += 1
            print(lieu)
            item = item | lieu
            yield scrapy.Request(
                structure_link,
                callback=self.parse_structure,
                meta={"item": item},
                dont_filter=True,
            )

    def parse_structure(self, response):

        item = response.meta.get("item")

        # Nom
        item["structure_nom"] = strip(
            response.css("div#structure > strong::text").get()
        )

        # Data màj
        item["structure_date_maj"] = strip(
            response.css("div.structures-dates > div:nth-child(2)")
            .xpath("text()")
            .get()
        )
        item["structure_date_maj"] = item["structure_date_maj"].split(" : ")[-1]
        item["structure_date_maj"] = dateparser.parse(
            item["structure_date_maj"]
        ).isoformat()

        # Adresse
        # Sur le site Web, une structure a autant d'adresses qu'elle a
        # de lieux pour ses services
        # Certains services sont proposés sur toutes les adresses de
        # la structure, certains non.

        # Téléphone
        telephone = response.css("div.lieu div.telephone > a::attr(href)").get()
        if type(telephone) == str:
            # Les numéro de téléphone sont préfixés par tel:
            telephone = telephone.strip()[4:]
        else:
            telephone = ""
        item["structure_telephone"] = telephone

        # Site Web
        item["structure_site_web"] = strip(
            response.css("div.lieu div.facebook::text").get()
        )

        # Lien source
        item["structure_lien_source"] = response.url

        # Labels
        item["structure_labels_autres"] = ["reseau-alpha"]

        # Thématiques
        item["structure_thematiques"] = ["apprendre-francais--suivre-formation"]

        return item


process = CrawlerProcess(
    settings={
        "FEEDS": {
            # Seul le JSON est utilisable dans le pipeline car le
            # CSV imprime les listes sans square brackets ([])
            # Le CSV est pratique pour tester
            "alpha.json": {
                "format": "json",
                "overwrite": True,
                "ensure_ascii": False,
                "encoding": "utf8",
                "store_empty": False,
            },
            "alpha.csv": {
                "format": "csv",
                "overwrite": True,
                "encoding": "utf8",
            },
        },
    }
)


processor = Processor(settings=None)


def extract(url: str, **kwargs):

    job = Job(AlphaSpider, url=url)
    data = processor.run(job)
    with io.StringIO() as buf:
        json.dump(data, buf)
        return buf.getvalue().encode()


# utils.read_json is enough to read the result
# later processing will be required to clean the presentation_detail
# fields when we extract them
