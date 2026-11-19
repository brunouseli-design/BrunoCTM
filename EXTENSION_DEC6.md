# Estensione autorizzata al servizio 6 dicembre 2026

Periodo collector: 8 novembre - 6 dicembre inclusi (29 giornate). Conservazione del 7 novembre invariata. Ogni servizio termina secondo il proprio statico congelato con buffer, incluse ore oltre 24. Safety ceiling in assenza di statico: 7 dicembre ore 08 Europe/Rome. Supervisione e lettori fino al 7 dicembre ore 12 per le elaborazioni conclusive.

Entrambi feed ogni 30 s best effort, sincronizzati header + 30 + 6. Nessuna nuova interpretazione delle assenze. Stesse risorse: 0.5c-512mb, disco 8 GB, nessuna cancellazione. Soglia di capacità 400 MB invariata. Statistiche e CSV giornalieri continuano. I vecchi ZIP fino al 21 novembre sono conservati; dal 22 i dati restano originali nelle cartelle delle giornate, senza duplicarli automaticamente in ulteriori ZIP sul disco. Esportazioni ZIP complete vanno generate esternamente o in streaming per mantenere margine.

Dopo installazione dei reader privati aggiornati (non includerli in questo repository pubblico), un deploy deve verificare runtime v2, 29 giornate, gli stessi lock e campioni dei due feed. L'eventuale raccolta temporanea durante il passaggio deve mantenere timestamp originali, provenienza e deduplica; non attribuire copertura a intervalli non acquisiti. Il report separato 8-21 resta distinto e non arresta la nuova raccolta.
