{{ unnest(
    from=ref('marts__services'),
    column='modes_orientation_accompagnateur',
    foreign_key='_di_surrogate_id',
    fk_alias='service_surrogate_id'
) }}
