# Aggiornamento autorizzato del 23 novembre 2026: termine 31 dicembre

Questa nota sostituisce soltanto le precedenti scadenze e risorse riportate sotto.
- Ultima giornata di servizio: 31 dicembre 2026, incluse le notturne e il buffer di 30 minuti determinati dallo statico congelato.
- In assenza dello statico, tetto provvisorio: 1 gennaio 2027 ore 08:00 Europe/Rome. Supervisione ed elaborazioni conclusive fino alle 12:00 dello stesso giorno.
- Piano autorizzato: 1 CPU / 2 GB RAM (`1c-2g`); disco persistente 11 GB su `/var/data`.
- Il lettore flotta privato eredita dal supervisore il medesimo termine; invariati filtri, soglie e metodo degli indicatori.
- BARMOZ resta sospeso: nessuna riattivazione, modifica ai suoi calcoli o integrazione nell'originale.
- Conservati archivi e precedenti risultati; nessuna cancellazione e nessun ripristino di snapshot. La disponibilità di spazio deve essere controllata durante il periodo esteso.
- Prima dei cambi di istanza è attiva una raccolta temporanea esterna dei due feed. Le sue risposte sono conservate separatamente, con provenienza esplicita, e non sono considerate automaticamente già integrate nello storico.
- Validazione locale: 25 test superati, compresi chiusura finale oltre mezzanotte, cambio mese, lock e tutela raccolta in caso di errori o memoria insufficiente.

## Nota precedente: estensione al 6 dicembre (storica)

# Estensione autorizzata al servizio 6 dicembre 2026

Periodo collector: 8 novembre - 6 dicembre inclusi (29 giornate). Conservazione del 7 novembre invariata. Ogni servizio termina secondo il proprio statico congelato con buffer, incluse ore oltre 24. Safety ceiling in assenza di statico: 7 dicembre ore 08 Europe/Rome. Supervisione e lettori fino al 7 dicembre ore 12 per le elaborazioni conclusive.

Entrambi feed ogni 30 s best effort, sincronizzati header + 30 + 6. Nessuna nuova interpretazione delle assenze. Stesse risorse: 0.5c-512mb, disco 8 GB, nessuna cancellazione. Soglia di capacità 400 MB invariata. Statistiche e CSV giornalieri continuano. I vecchi ZIP fino al 21 novembre sono conservati; dal 22 i dati restano originali nelle cartelle delle giornate, senza duplicarli automaticamente in ulteriori ZIP sul disco. Esportazioni ZIP complete vanno generate esternamente o in streaming per mantenere margine.

Dopo installazione dei reader privati aggiornati (non includerli in questo repository pubblico), un deploy deve verificare runtime v2, 29 giornate, gli stessi lock e campioni dei due feed. L'eventuale raccolta temporanea durante il passaggio deve mantenere timestamp originali, provenienza e deduplica; non attribuire copertura a intervalli non acquisiti. Il report separato 8-21 resta distinto e non arresta la nuova raccolta.
