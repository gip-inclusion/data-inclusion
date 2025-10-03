{{ unnest(
    from=ref('marts__structures_v1'),
    column='thematiques',
    foreign_key='id',
    fk_alias='structure_id'
) }}
