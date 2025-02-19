WITH raw AS (
    SELECT
xml $$
<dict name="dict-perimetre-recrutement">
    <fullname xml:lang="fr">Périmètre de recrutement</fullname>
    <entry>
        <key val="0"/>
        <value xml:lang="fr" val="Autres"/>
    </entry>
    <entry>
        <key val="1"/>
        <value xml:lang="fr" val="Commune"/>
    </entry>
    <entry>
        <key val="2"/>
        <value xml:lang="fr" val="Département"/>
    </entry>
    <entry>
        <key val="3"/>
        <value xml:lang="fr" val="Région"/>
    </entry>
    <entry>
        <key val="4"/>
        <value xml:lang="fr" val="Interrégion"/>
    </entry>
    <entry>
        <key val="5"/>
        <value xml:lang="fr" val="Pays"/>
    </entry>
    <entry>
        <key val="6"/>
        <value xml:lang="fr" val="International"/>
    </entry>
    <property name="glb:service" value="perimetre-recrutement" />
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
