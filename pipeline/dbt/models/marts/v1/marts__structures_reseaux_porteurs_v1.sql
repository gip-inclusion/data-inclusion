{{ unnest(
    from=ref('marts__structures'),
    column='thematiques',
    foreign_key='id',
    fk_alias='structure_id'
) }}
