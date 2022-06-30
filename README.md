
# Scalable-DBSCAN



## Introduzione

Il progetto proposto è incentrato su DBSCAN, un algoritmo di clustering proposto
da Martin Ester, Hans-Peter Kriegel, J ̈org Sander e Xiaowei Xu nel 1996.

Tramite il linguaggio Scala, affiancato dal framework per il calcolo distribuito Apache Spark,
è stata implementata una versione parallela e distribuita dell'algoritmo sopracitato sulla quale è stato 
effettutato uno studio relativo alle performance da esso ottenute.



## Data Preprocessing e Tuning Parametri
Il dataset utilizzato nei test relativi al calcolo delle performance dell'algoritmo
è l' [Household Electric Power Consuption](https://www.kaggle.com/uciml/electric-power-consumption-data-set)
il quale contiene misurazioni relative al consume elettrico di una abitazione campionata minuto per minuto per 4 anni.

Inizialmente i dati sono stati pre-processati usando la tecnica denominata Principal Component Analysis (PCA) sulle features 
dalla numero 3 alla 9 per ottenere un insieme di punti bidimensionali da dare in input all'algoritmo DBSCAN.

Prima di eseguire l'algoritmo DBSCAN, è stato necessario eseguire una versione semplificata di KNN, che calcola solo le distanze tra i punti, 
attraverso cui si è stati in grado di determinare il parametro epsilon (uno degli iperparametri richiesti) ottimale per ottenere la migliore "clusterizzazione" possibile.

## Scalable-DBSCAN
Scalable-DBSCAN è un algoritmo sviluppato per consentire il clustering di un
gran numero di dati in maniera distribuita.

- Il primo step consiste nel disporre i dati in rettangoli (boxes) bilanciati
- In seguito, ogni box viene espansa secondo un certo valore γ, in modo da includere al suo interno tutti i punti che si trovano a distanza γ da essa
- Viene eseguito l'algoritmo DBSCAN in parallalelo su ogni box
- Una volta esaminati tutti i punti, se uno di essi è stato etichettato come parte di due cluster diversi, questi ultimi verranno fusi insieme e considerati come un unico gruppo (Reduce Phase)
- Tutti i restanti punti vengono assegnati ai nuovi cluster di appartenza,
selezionati a partire da quelli ottenuti nella fase di riduzione

L'implementazione di DBSCAN presentata, è costituita da 3 fasi distinte:
- **Data Manipulation**: divide l’intero set di dati in partizioni più piccole in base alla vicinanza spaziale
- **Local Clustering**: in questa fase DBSCAN viene eseguito localmente su ogni cluster, il quale genererà dei risulati parziali
- **Partial Results Merging and Aggregating**: infine i risultati parziali ottenuti nella fase di clustering vengono aggregati per generare il risultato finale che il sistema produrrà in output

## Cloud e Risultati
Per valutare le performance ottenute dall'implementazione dell'algoritmo Scalable-DBSCAN sono state utilizzate differenti partizioni del dataset principale di diversa cardinalità:

- 10.000 osservazioni
- 25.000 osservazioni
- 50.000 osservazioni
- 75.000 osservazioni
- 100.000 osservazioni
- 150.000 osservazioni

Per ognuna delle partizione appena descritte, si è eseguito l'algoritmo su macchine con differenti architetture, sia in locale che in Cloud (Google Cloud)
Le performance ottenute sul Cloud non sono del tutto migliori di quelle misurate in locale.

Dai risultati ottenuti si evince che il sistema sia riuscito ad effettuare l'operazione di clustering in maniera corretta,
riconoscendo quindi i gruppi di appartenenza corretti per ciascun elemento, 
e le sue performance sono nettamente migliorate rispetto all'implementazione di libreria di DBSCAN.

## Requisiti ed Esecuzione
Requisiti:
- Scala 2.12.14
- Apache Spark 3.1.0
- Hadoop 3.2
- Python 3.7.13+ (Google Colab)

Esecuzione: 
- Scaricare  [Household Electric Power Consuption Dataset](https://www.kaggle.com/uciml/electric-power-consumption-data-set) 
- Convertire il dataset da .txt a .csv
- Creare una cartella denominata "preproc_out" sul proprio account Google Drive e fare l'upload del dataset appena scaricato al suo interno
- Aprire il notebook Google Colab al seguente [link](https://colab.research.google.com/drive/1E_QJMsbXoVeEupGUB3vLZ28tWC1qybqQ#scrollTo=kfKXKjYkEWdF) per generare le diverse partizioni del dataset utilizzate
- Eseguire l'algoritmo mKNN per ottenere il valore di epsilon ottimale da fornire a Scalable-DBSCAN (facoltativo in quanto già calcolati)
- Creare il file .jar relativo al progetto eseguendo il comando "package" nella shell di scala, posizionandosi all'interno della root del progetto
- Eseguire il file .jar appena generato fornendo in input i seguenti parametri nell'ordine di seguito specificato (nel caso di esecuzione su cloud considerare il path relativo al file system dello storage del servizio cloud utilizzato): 
  - path del dataset in input
  - path file di output
  - numero di thread 
  - valore di epsilon


Esecuzione su cloud:
- Creare un bucket che contenga i file di input e di output e il file jar 
- Creare i cluster su cui si desidera testare l'esecuzione settandoli con numero arbitrario di master e nodi
- Crere il job dove si specifica il cluster, il path del jar e i parametri in input
- Avviare l'esecuzione del cluster e del job 
