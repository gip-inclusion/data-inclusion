WITH raw AS (
    SELECT
xml $$
<dict name="dict-modalites-enseignement">
    <fullname xml:lang="fr">Formation présentielle ou a distance</fullname>
    <entry>
        <key val="0"/>
        <value xml:lang="fr" val="formation entièrement présentielle"/>
    </entry>
    <entry>
        <key val="1"/>
        <value xml:lang="fr" val="formation mixte"/>
    </entry>
    <entry>
        <key val="2"/>
        <value xml:lang="fr" val="formation entièrement à distance"/>
    </entry>
    <property name="glb:service" value="modalites-enseignement" />
</dict>
$$ AS data
),

final AS (
    SELECT
        xmltable.*
    FROM
        raw,
        XMLTABLE(
            '//dict/entry'
            PASSING data
            COLUMNS key TEXT PATH 'key/@val',
                    value TEXT PATH 'value/@val'
        )
)

SELECT * FROM final
