{{ unnest(
    from=ref('marts__services_v1'),
    column='modes_accueil',
    foreign_key='id',
    fk_alias='service_id'
) }}
