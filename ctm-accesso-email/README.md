# CTM · Accesso indipendente con codice email

Stato: software preparato e verificato in locale; non pubblicato, invio reale non configurato.

Portale di sola consultazione, indipendente dagli account OpenAI. Il visitatore inserisce un indirizzo autorizzato, riceve un codice di 8 cifre valido 10 minuti e lo usa una sola volta. Dopo 5 tentativi errati deve richiedere un nuovo codice. La sessione scade dopo 8 ore ed è revocabile eliminando l'indirizzo dall'elenco autorizzato e riavviando il portale. Un nuovo accesso invalida la precedente sessione dello stesso indirizzo.

## Separazione dall'originale

Questo ramo aggiunge esclusivamente la cartella `ctm-accesso-email`. Non modificare `app.py` nella radice del repository, il ramo `main`, il servizio CTM esistente, la sua archiviazione o le autorizzazioni del Site originale.

Il portale interroga il Site originale attraverso una credenziale di servizio conservata solo nell'ambiente del nuovo servizio. La credenziale non deve comparire nel codice, nel browser, nei log o in questo documento. Il visitatore può consultare soltanto una lista esplicita di pagine/API con richieste GET. Le funzioni amministrative e le richieste di scrittura restano escluse. Le analisi richiamate in lettura dal Site possono usare le proprie normali cache già previste dal programma originale; il portale non cambia quelle logiche né avvia archiviazioni alternative.

Il passaggio intermedio è applicato anche alle chiamate API: senza sessione verificata non vengono interrogati i dati originali. Nessun collegamento di accesso contiene credenziali. Non sono inoltrati al Site cookie o intestazioni scelti dal visitatore. Le risposte originali non possono impostare cookie nel portale e i reindirizzamenti del servizio originale non vengono seguiti.

## Parametri da configurare sul nuovo servizio

| Chiave | Contenuto |
| --- | --- |
| `PUBLIC_ORIGIN` | Origine HTTPS del nuovo portale, senza barra finale |
| `ALLOWED_EMAILS` | Elenco degli indirizzi espressamente autorizzati, separati da virgole |
| `SESSION_SECRET` | Segreto casuale di almeno 32 caratteri, generato e conservato nel gestore dei segreti |
| `UPSTREAM_TOKEN` | Credenziale di servizio del Site originale; mai consegnarla al visitatore |
| `AUTH_DATABASE` | File SQLite su disco persistente **separato** da quello del servizio CTM |
| `SMTP_HOST` | Server del mittente autorizzato a spedire codici |
| `SMTP_PORT` | 587 con STARTTLS oppure 465 con TLS |
| `SMTP_FROM` | Indirizzo mittente autorizzato |
| `SMTP_USER`, `SMTP_PASSWORD` | Credenziali di un account tecnico SMTP, non della casella del visitatore |

Sono richiesti SMTP autenticato e TLS con certificati verificati. Un ambiente Microsoft 365 con autenticazione SMTP di base disabilitata richiederà un adattatore OAuth o un servizio di email transazionali compatibile: non indebolire le policy aziendali per usare questa implementazione. La disponibilità di SMTP, i parametri di un mittente e l'eventuale costo del servizio di posta vanno accertati prima della pubblicazione.

## Avvio e verifiche

Python 3.11 o successivo, nessuna dipendenza esterna. Directory di lavoro: `ctm-accesso-email`. Comando di avvio: `python app.py`. Render fornisce `PORT`; endpoint di salute: `/health`.

Eseguire `python -m unittest -v test_app` prima della pubblicazione. I test usano un mittente simulato e una fonte simulata: non inviano email e non interrogano il servizio CTM reale.

Su Render, creare un servizio distinto che usi il ramo `accesso-email-otp`, directory `ctm-accesso-email`, con disco persistente proprio per SQLite. Non riutilizzare il servizio esistente e non attivare deployment automatici sul ramo `main`. Prima di creare risorse a pagamento, concordarne il piano. `/health` verifica il processo, non la consegna SMTP o il collegamento effettivo al Site.

Prima dell'uso reale verificare: connessione alla fonte privata con credenziale di servizio; ricezione del codice nella casella autorizzata; accesso e navigazione di tutte le viste; codice errato, scaduto e riutilizzato; diniego delle richieste anonime; uscita da `/esci`; assenza di segreti e codici nei log. Non presentare il portale come attivo finché questi passaggi non sono superati.

L'accesso dimostra il controllo della casella autorizzata. Se il titolare condivide la casella, il codice o il dispositivo già autenticato, la verifica email non distingue le persone.
