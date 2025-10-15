from pathlib import Path


def extract(url: str, **kwargs) -> bytes:
    import base64
    import io

    import furl
    import paramiko

    # the url should contain a proper ssh url with all
    # the necessary information (host, port, username, password, ...)
    url_obj = furl.furl(url)

    # The 'AAAA....' would already be a base64 encoding of the raw binary host key,
    # but since we store it in an URL we want to avoid any issues with special
    # characters such as / or +, so we encoded it again in base64.
    host_key = base64.b64decode(url_obj.query.params.get("host_key"))

    ssh_client = paramiko.SSHClient()
    ssh_client.get_host_keys().add(
        f"[{url_obj.host}]:{url_obj.port}",
        "ssh-rsa",
        paramiko.RSAKey(data=base64.b64decode(host_key)),
    )
    ssh_client.connect(
        hostname=url_obj.host,
        port=url_obj.port,
        username=url_obj.username,
        password=url_obj.password,
    )

    sftp_client = ssh_client.open_sftp()
    with io.BytesIO() as buf:
        sftp_client.getfo(remotepath=str(url_obj.path), fl=buf)
        return buf.getvalue()


def read(path: Path):
    from pathlib import Path

    import pandas as pd
    import xmlschema

    from . import utils

    schema_path = Path(__file__).resolve().parent / "lheo.xsd"
    schema = xmlschema.XMLSchema(schema_path)
    data = schema.to_dict(path)

    for formation_data in data["offres"]["formation"]:
        formation_data["objectif-formation"] = utils.html_to_markdown(
            formation_data["objectif-formation"]
        )
        formation_data["contenu-formation"] = utils.html_to_markdown(
            formation_data["contenu-formation"]
        )

    df = pd.json_normalize(
        data=data,
        record_path=["offres", "formation"],
        max_level=0,
    )
    return utils.df_clear_nan(df)
