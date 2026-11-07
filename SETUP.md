# Archivio CTM — giornata di servizio 7 novembre 2026

Servizio pubblicato su Render per la giornata di servizio del 7 novembre 2026.

## Funzionamento

- Un singolo servizio Python con disco persistente da 1 GB.
- Due richieste parallele a intervalli nominali di 30 secondi, allineati ai timestamp CTM più 6 secondi per la pubblicazione. Timestamp assenti o obsoleti: fallback di 30 secondi.
- Il 7 novembre conserva una copia del GTFS statico Flashnet, quindi ricava dai
  calendari e dalle eccezioni le corse attive e l'ultimo orario programmato.
- La finestra inizia alle 00:00 Europe/Rome del 7 novembre e termina almeno
  a mezzanotte, oppure 30 minuti dopo l'ultima corsa se successiva.
  Le ore GTFS superiori a 24 rimangono nella giornata di servizio corretta.
- Ogni risposta viene conservata in Protobuf compresso, con metadati JSON,
  timestamp di richiesta, header del feed, hash SHA-256 e log degli errori.
- I buchi di raccolta non vengono ricostruiti o nascosti. Un riavvio riprende
  dai file presenti. La raccolta non recupera retroattivamente dati mancanti.
- Alla fine della finestra produce ZIP, JSON e CSV per linea e singolo evento.
  Il processo smette di interrogare CTM e lascia disponibili i download.

## Statistiche

Copertura delle corse programmate, veicoli distinti, errori e campioni mancanti,
dati obsoleti, associazioni fallite e distribuzione dei ritardi previsti per linea.
Per evitare di pesare più volte lo stesso evento, viene usata l'ultima previsione
fresca disponibile prima dell'evento previsto, entro un'ora dall'orario statico.
Arrivo e partenza sono eventi distinti. Le corse senza start_date sono escluse;
gli ID non abbinabili vengono contati, senza inventare corrispondenze.

**Le previsioni non provano un passaggio realmente avvenuto.** L'assenza di un
mezzo nel feed non prova una cancellazione. La puntualità effettiva richiede
una successiva analisi delle osservazioni e delle loro caratteristiche.

## Pubblicazione

Caricare questi file nella radice di un repository GitHub accessibile a Render.
Creare un Blueprint dal render.yaml; verificare costi e piano prima di applicarlo.
La configurazione richiede un servizio a pagamento e un disco persistente.
Il piano approvato comprende 512 MB RAM e disco da 1 GB.

Render genera ACCESS_PASSWORD. Accedere all'URL del servizio con utente `ctm`
e quella password, reperibile nelle variabili del servizio. Non pubblicare la
password nel repository o nei log. `/health` è pubblico e non espone dati.

I download protetti sono `/archive.zip`, `/statistics.json`, `/routes.csv`.
Il processo HTTP resta attivo dopo la raccolta: sospendere il servizio dopo aver
scaricato e messo al sicuro i dati. I costi non terminano con la sola fine del polling.

## Verifica locale

Python 3.12 o successivo con fuso Europe/Rome disponibile:

```sh
pip install -r requirements.txt
python -m unittest -v
```

Il cambio da 60 a 30 secondi è registrato in sampling-changes.json e nei log CTM_SAMPLING_CHANGE. I campioni già raccolti sono conservati; eventuali buchi di deploy sono misurati dai timestamp. Il conteggio nominale delle richieste tiene conto delle due frequenze e può differire dalle pubblicazioni effettive CTM.

Fonte: CTM S.p.A., https://flashnetctm.miz.it/download/gtfs/
Il GTFS statico è pubblicato da CTM con licenza CC BY 3.0 Italia.
Non viene attribuita automaticamente tale licenza ai feed realtime.
