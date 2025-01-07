# **EEG Power Band Analysis using Hadoop and Python**

Ta projekt omogoča analizo EEG signalov z uporabo **Hadoop MapReduce** okvira za analizo velikih podatkov in **Python Notebook** za lokalno analizo in preverjanje rezultatov.

---

## **Mapa z vsebino**
- **src/main/**  
   Vsebuje implementacijo Mapper in Reducer razredov ter `FTDriver.java` za zagon Hadoop joba.
   
- **input/**  
   Vsebuje vhodne podatke (`filtered_eeg_data.csv`) z EEG signali za obdelavo.  
   Format:
   ```
   electrode1,-0.1,0.04,0.2,...
   electrode2,0.2,-0.05,-0.1,...
   ```

- **output/**  
   Tu so shranjeni rezultati iz Hadoop joba v obliki CSV datoteke (`part-r-00000-norm.csv`).  
   Format:
   ```
   Electrode,Delta,Theta,Alpha,Beta,Gamma
   electrode1,0.70,0.15,0.10,0.04,0.01
   electrode2,0.68,0.16,0.11,0.03,0.02
   ```

- **python_notebook/**  
   Ta mapa vsebuje **Python Notebook**, ki:
     - Pretvori `Janez_Bucar.mat` datoteku v ustrezen csv format (`filtered_eeg_data.csv`)
     - Bere rezultate MapReduce `part-r-00000-norm.csv`.
     - Vključuje vizualizacijo rezultatov za hitro analizo.

---

## **Primer uporabe**
1. Uporabi Hadoop job za obdelavo velikih podatkov (datoteka prikazana v `src/main/`).   ```bash hadoop jar /tmp/JanezEEG-1.0-SNAPSHOT.jar org.main.FTDriver /user/input /user/output ```

2. Za lokalno preverbo in vizualizacijo uporabite datoteke v **python_notebook/** mapi.

---

## **Avtor**
Projekt je razvil **jANEZ BUČAR**. Za vprašanja nas lahko kontaktirate preko e-pošte **GOSPOD.BUCARRR@GMAIL.COM**.
