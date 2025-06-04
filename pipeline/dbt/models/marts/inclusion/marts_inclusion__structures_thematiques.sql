{{ unnest(
    from=ref('marts_inclusion__structures_v0'),
    column='thematiques',
    foreign_key='_di_surrogate_id',
    fk_alias='structure_surrogate_id'
) }}
