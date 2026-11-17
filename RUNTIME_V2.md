# Avvio sorvegliato e realtime indipendente dallo statico

## Stato della modifica

Preparata e validata il17 novembre2026 partendo dal ramo main96177b4a42ded0be5d7a17e94e6cb892a61c6206. La modifica del repository non è un deploy: auto-deploy Render è disattivato. Non sono cambiati file, configurazioni o processi in esecuzione sul disco Render. Non attivare dalla Shell durante la raccolta.

L'ingresso ufficiale resta `python app.py`: alla prossima pubblicazione esplicitamente autorizzata avvierà automaticamente `runtime_bootstrap.py`. Nessuna modifica del comando Render, del Blueprint, delle risorse o delle variabili segrete è necessaria. Il disco resta8GB e il piano resta0.5CPU/512MB.

## Avvio e recupero

Il processo HTTP avvia un supervisore e ne verifica la presenza. Il supervisore ripristina solo i propri figli assenti; rispetta i lock già esistenti. Usa il lock del collector originale `/var/data/ctm-extension/runner.lock`, evitando due scrittori sullo stesso archivio. Un collector preesistente vivo non viene terminato, sostituito o affiancato da un secondo writer. I lettori privati rimangono nei loro file originali sul disco: vengono avviati solo se file e lock lo consentono.

La nuova implementazione comprende acquisizione, processi opzionali per statico e report, lettori e heartbeat su `/api/operations`, usando la configurazione privata già presente e senza loggare token o password. Un supervisore già attivo viene rispettato. In caso di arresto vengono terminati solo figli avviati da quella stessa istanza, mai processi preesistenti.

La raccolta resta8–21 novembre. Il7 non entra nel nuovo motore. Gli orari finali e il buffer vengono letti dal piano congelato di ciascuna giornata. Quando lo statico manca, il realtime grezzo continua: un tetto provvisorio di sicurezza viene dichiarato esplicitamente e non viene spacciato per termine GTFS. Tutta l'acquisizione termina comunque entro22nov07UTC/08Roma; l'esecuzione dei componenti aggiuntivi non è prolungata oltre6dicembre. I promemoria e il report finale esistenti non vengono modificati.

## Statica e raw RT separati

`collection_runtime.py` interroga entrambi i feed ogni30s, con timestamp reali, metadati degli errori e sincronizzazione header+30+6s best effort. Gli errori dello statico, inclusi timeout e mancato avvio del worker, non impediscono le richieste RT. Vengono conservati anche i campioni vuoti e gli errori HTTP: non significano servizio assente.

Lo statico viene scaricato in un processo opzionale separato, non più frequentemente di4h; il tentativo viene registrato prima della richiesta. Viene congelato esclusivamente nella sua data di servizio. Un giorno successivo non viene usato per ricostruire retroattivamente lo statico mancante.

SQLite viene costruito in un file derivato temporaneo, controllato con quick_check, poi pubblicato per rename; plan.json viene scritto per ultimo. In assenza di statico validato i raw vengono conservati, ma il confronto programmato/osservato resta indisponibile. I lettori esistenti eseguiranno l'abbinamento quando saranno disponibili statico e piano.

Statico, report e processi di analisi flotta condividono un lock per le lavorazioni pesanti. Prima di avviare lavoro opzionale viene verificata una riserva di160MiB usando anon+kernel e non il semplice totale comprensivo di cache. Lo statico/report ha un limite di128MiB; l'analisi flotta conserva il proprio limite di96MiB. Se la riserva manca il lavoro viene rinviato, non viene arrestato il collector. Lo storico originale e la configurazione privata non vengono copiati nel repository.

## Validazione prima della modifica del progetto

20 test automatici passati in ambiente isolato:17 nuovi e3 regressioni originali. Nessuna chiamata ai feed CTM reali nei test e nessun processo su Render avviato o segnalato.

- Statico assente, errore, preparazione scaduta e mancato avvio del worker: entrambi i feed continuano a produrre metadati e raw disponibili.
- Rispetto dei4h e divieto di scaricare uno statico successivo attribuendolo a ieri.
- Pubblicazione di statico già congelato mediante vero sottoprocesso locale limitato, con SQLite completo e integro.
- Corsa oltre24:00, fine alle03:20 e buffer fino03:50.
- Isolamento delle giornate e conservazione byte per byte del file sentinella del7.
- Lock di un collector preesistente: nessun secondo writer.
- Server HTTP locale:503 su TripUpdates mentre VehiclePositions funziona, poi ripresa; tutti e quattro i tentativi sono conservati nei metadati.
- Mancanza di memoria e lock pesante occupato: rinvio prima dell'analisi.
- Recupero di veri sottoprocessi locali terminati, nessun duplicato di figli vivi, nessuna terminazione di processi preesistenti.
- Simulazione di due cicli di avvio con figli reali e conservazione dell'archivio temporaneo. È una simulazione del ciclo di vita, non un riavvio reale Render.
- Esecuzione dell'ingresso effettivo app.py con server e thread simulati: viene chiamato il bootstrap attraverso il comando già configurato.
- Import dei moduli senza avviare processi; archivio vuoto segnalato come dato mancante.
- Regressioni originali: sincronizzazione, calendario/eccezioni/notturne e statistiche previsionali.

## Attivazione futura: controllo obbligatorio

Per rispettare l'istruzione di non toccare processi esistenti, questa versione non viene pubblicata su Render e non viene avviata dalla Shell. La protezione nuova diventerà operativa solo dopo un futuro deploy autorizzato e verificato. Prima del deploy servono una raccolta di appoggio funzionante, verifica del suo salvataggio, e verifica dei file privati dei lettori presenti sul disco.

Il collector, il guardian e i lettori attuali restano invariati. Alla futura attivazione va aggiornato anche il controllo esterno affinché riconosca `/var/data/ctm-runtime-v2/state.json` e ripristini il bootstrap del repository, senza avviare il guardian v1 in concorrenza. Dopo il deploy verificare i due feed archiviati, heartbeat, PID, lock, qualità del confronto e RAM prima di dismettere l'appoggio.
