{{ unnest(
    from=ref('marts__services'),
    column='modes_mobilisation',
    foreign_key='id',
    fk_alias='service_id'
) }}
