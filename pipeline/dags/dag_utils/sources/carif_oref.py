from pathlib import Path


def extract(url: str, **kwargs) -> bytes:
    import io

    import furl
    import paramiko

    # the url should contain a proper ssh url with all
    # the necessary information (host, port, username, password, ...)
    url_obj = furl.furl(url)

    ssh_client = paramiko.SSHClient()
    # TODO(vmttn): rather than using the auto add policy,
    # the host key could be base64 encoded as a query param in the ssh url
    # and added thanks to ssh_client.get_host_keys().add(...)
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
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

    df = pd.json_normalize(
        data=data,
        record_path=["offres", "formation"],
        max_level=0,
    )
    return utils.df_clear_nan(df)
