WITH raw AS (
    SELECT
xml $$
<dict name="dict-type-contact">
    <fullname xml:lang="fr">Type de contact</fullname>
    <entry>
        <key val="0"/>
        <value xml:lang="fr" val="Autre"/>
    </entry>
    <entry>
        <key val="1"/>
        <value xml:lang="fr" val="Référent handicap"/>
    </entry>
    <entry>
        <key val="2"/>
        <value xml:lang="fr" val="Référent mobilité internationale"/>
    </entry>
    <entry>
        <key val="3"/>
        <value xml:lang="fr" val="Référent pédagogique"/>
    </entry>
    <entry>
        <key val="4"/>
        <value xml:lang="fr" val="Accueil"/>
    </entry>
    <entry>
        <key val="5"/>
        <value xml:lang="fr" val="Direction"/>
    </entry>
    <entry>
        <key val="6"/>
        <value xml:lang="fr" val="Référent qualité"/>
    </entry>
    <entry>
        <key val="7"/>
        <value xml:lang="fr" val="Référent entreprise"/>
    </entry>
    <entry>
        <key val="8"/>
        <value xml:lang="fr" val="Référent commercial"/>
    </entry>
    <entry>
        <key val="9"/>
        <value xml:lang="fr" val="Gestionnaire financier"/>
    </entry>
    <entry>
        <key val="10"/>
        <value xml:lang="fr" val="Direction Déléguée aux Formations Professionnelles et Techniques (DDFPT)"/>
    </entry>
    <entry>
        <key val="11"/>
        <value xml:lang="fr" val="Communication"/>
    </entry>
    <entry>
        <key val="12"/>
        <value xml:lang="fr" val="Référent FOAD"/>
    </entry>
    <entry>
        <key val="13"/>
        <value xml:lang="fr" val="Référent VAE"/>
    </entry>
    <entry>
        <key val="14"/>
        <value xml:lang="fr" val="Référent Apprentissage"/>
    </entry>
    <property name="glb:service" value="financeurs" />
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
