# handle dataframes
import pandas as pd
# DB connection
import sqlalchemy

"""This file is used to retrieve datasets needed to train failure prediction ML model from a PostgreSQL database
"""

DATABASE_URI = "postgresql://marie@localhost:5432/fail_prediction"

def get_dataset_db(DATABASE_URI = "postgresql://marie@localhost:5432/fail_prediction"):
    """Gets a df holding the dataset used to perform failure prediction (french companies accounts of year 2018) from a PostgreSQL database

    Args:
        DATABASE_URI (str, optional): URI of database with format as follow : f"postgresql://{DB_USERNAME}@{DB_ADDRESS}:{DB_PORT}/{DB_NAME}"
        Defaults to "postgresql://marie@localhost:5432/fail_prediction".

    Returns:
        pandas.DataFrame: df holding the dataset used to perform failure prediction (french companies accounts of year 2018 - 132 columns including the target)
    """
    dataset_cols = ['SIREN',
                 'Dénomination',
                 'SIRET',
                 'Code APE',
                 'Procédures collectives (type)',
                 'Procédures collectives (date)',
                 'Code postal',
                 'Date de création',
                 'Tranche effectifs',
                 'Capital souscrit non appelé (I) (AA) 2018 (€)',
                 'TOTAL (II) (net) (BJNET) 2018 (€)',
                 'Matières premières, approvisionnements (net) (BLNET) 2018 (€)',
                 'En cours de production de biens (net) (BNNET) 2018 (€)',
                 'En cours de production de services (net) (BPNET) 2018 (€)',
                 'Produits intermédiaires et finis (net) (BRNET) 2018 (€)',
                 'Marchandises (net) (BTNET) 2018 (€)',
                 'Avances et acomptes versés sur commandes (net) (BVNET) 2018 (€)',
                 'Clients et comptes rattachés (3) (net) (BXNET) 2018 (€)',
                 'Autres créances (3) (net) (BZNET) 2018 (€)',
                 'Capital souscrit et appelé, non versé (net) (CBNET) 2018 (€)',
                 'Valeurs mobilières de placement (net) (CDNET) 2018 (€)',
                 'Disponibilités (net) (CFNET) 2018 (€)',
                 "Charges constatées d'avance (3) (net) (CHNET) 2018 (€)",
                 'TOTAL (III) (net) (CJNET) 2018 (€)',
                 'Primes de remboursement des obligations (CM) 2018 (€)',
                 'Ecarts de conversion actif (CN) 2018 (€)',
                 'TOTAL GENERAL(I à VI) (net) (CONET) 2018 (€)',
                 'Capital social ou individuel (1) (DA) 2018 (€)',
                 'Report à nouveau (DH) 2018 (€)',
                 "RESULTAT DE L'EXERCICE (bénéfice ou perte) (DI) 2018 (€)",
                 'TOTAL (I) (DL) 2018 (€)',
                 'TOTAL(II) (DO) 2018 (€)',
                 'TOTAL (III) (DR) 2018 (€)',
                 'Autres emprunts obligataires (DT) 2018 (€)',
                 'Emprunts obligataires convertibles (DS) 2018 (€)',
                 'Emprunts et dettes auprès des établissements de crédit (5) (DU) 2018 (€)',
                 'Emprunts et dettes financières divers (DV) 2018 (€)',
                 'Avances et acomptes reçus sur commandes en cours (DW) 2018 (€)',
                 'Dettes fournisseurs et comptes rattachés (DX) 2018 (€)',
                 'Dettes fiscales et sociales (DY) 2018 (€)',
                 'Dettes sur immobilisations et comptes rattachés (DZ) 2018 (€)',
                 'Autres dettes (EA) 2018 (€)',
                 "dont comptes courants d'associés de l'exercice N (EA2) 2018 (€)",
                 "dont comptes courants d'associés de l'exercice N (EA2) 2017 (€)",
                 "Produits constatés d'avance (EB) 2018 (€)",
                 'TOTAL (IV) (EC) 2018 (€)',
                 'TOTAL GENERAL (I à V) (EE) 2018 (€)',
                 '(5)\xa0Dont concours bancaires courants, et soldes créditeurs de banques et CCP (EH) 2018 (€)',
                 "Chiffre d'affaires net (France) (FJ) 2018 (€)",
                 "Chiffre d'affaires net (Exportations et livraisons intracommunautaires) (FK) 2018 (€)",
                 "Chiffre d'affaires net (Total) (FL) 2018 (€)",
                 "Chiffre d'affaires net (Total) (FL) 2017 (€)",
                 "Subventions d'exploitation (FO) 2018 (€)",
                 'Reprises sur amortissements et provisions, transferts de charges (9) (FP) 2018 (€)',
                 "Total des produits d'exploitation (2) (I) (FR) 2018 (€)",
                 'Achats de marchandises (y compris droits de douane) (FS) 2018 (€)',
                 'Variation de stock (marchandises) (FT) 2018 (€)',
                 'Achats de matières premières et autres approvisionnements (y compris droits de douane) (FU) 2018 (€)',
                 'Variation de stock (matières premières et approvisionnements) (FV) 2018 (€)',
                 'Autres achats et charges externes (3) (6 bis) (FW) 2018 (€)',
                 'Impôts, taxes et versements assimilés (FX) 2018 (€)',
                 'Salaires et traitements (FY) 2018 (€)',
                 'Charges sociales (10) (FZ) 2018 (€)',
                 "Dotations d'exploitation sur immobilisations (dotations aux amortissements) (GA) 2018 (€)",
                 "Dotations d'exploitation sur immobilisations (dotations aux provisions) (GB) 2018 (€)",
                 "Dotations d'exploitation sur actif circulant (dotations aux provisions) (GC) 2018 (€)",
                 "Dotations d'exploitation pour risques et charges (dotations aux provisions) (GD) 2018 (€)",
                 'Autres charges (12) (GE) 2018 (€)',
                 "Total des charges d'exploitation (4) (II) (GF) 2018 (€)",
                 "1 - RESULTAT D'EXPLOITATION (I - II) (GG) 2018 (€)",
                 'Total des produits financiers (V) (GP) 2018 (€)',
                 'Intérêts et charges assimilées (GR) 2018 (€)',
                 'Total des charges financières (VI) (GU) 2018 (€)',
                 'Dotations financières aux amortissements et provisions (GQ) 2018 (€)',
                 'Reprises sur provisions & transferts de charges (GM) 2018 (€)',
                 'Total des produits exceptionnels (VII) (HD) 2018 (€)',
                 'Reprises sur provisions & transferts de charges (HC) 2018 (€)',
                 'Dotations exceptionnelles aux amortissements et provisions (6 ter) (HG) 2018 (€)',
                 'Total des charges exceptionnelles (VIII) (HH) 2018 (€)',
                 '4 - RESULTAT EXCEPTIONNEL (VII - VIII) (HI) 2018 (€)',
                 '5 - BENEFICE OU PERTE (Total des produits - total des charges) (HN) 2018 (€)',
                 "Participation des salariés aux résultats de l'entreprise (HJ) 2018 (€)",
                 'Impôts sur les bénéfices (HK) 2018 (€)',
                 'Clients douteux ou litigieux - Montant brut (VA) 2018 (€)',
                 'Sécurité sociale et autres organismes sociaux - Montant brut (8D) 2018 (€)',
                 'Impôts sur les bénéfices - Montant brut (8E) 2018 (€)',
                 'T.V.A. - Montant brut (VW) 2018 (€)',
                 'Emprunts remboursés en cours d’exercice (VK) 2018 (€)',
                 'Emprunts souscrits en cours d’exercice - à 1 an au plus (VJ2) 2018 (€)',
                 "Emprunts souscrits en cours d’exercice - à plus d'un an et 5 ans au plus (VJ3) 2018 (€)",
                 'Emprunts souscrits en cours d’exercice - à plus de 5 ans (VJ4) 2018 (€)',
                 'Effets portés à l’escompte et non échus (YS) 2018 (€)',
                 'Sous‐traitance (YT) 2018 (€)',
                 'Ecarts de conversion passif (V) (ED) 2018 (€)',
                 'Production stockée (FM) 2018 (€)',
                 'Production immobilisée (FN) 2018 (€)',
                 '(3)\xa0Dont Crédit-bail mobilier (HP) 2018 (€)',
                 '(3)\xa0Dont Crédit-bail immobilier (HQ) 2018 (€)',
                 '3 - RESULTAT COURANT AVANT IMPOTS (I - II + III - IV + V - VI) (GW) 2018 (€)',
                 'Ville',
                 "Nombre de mois de l'exercice comptable 2018",
                 'Catégorie juridique (Niveau II)',
                 'default',
                 'Credit client',
                 'Credit Fournisseurs',
                 'Rotation_stocks',
                 'BFR',
                 'BFRE',
                 'Endettement total',
                 'CAF',
                 'Capacite de remboursement',
                 'Ressources durables',
                 'FRNG',
                 'Taux endettement',
                 'Rentabilite financiere',
                 'EBE',
                 'VA',
                 'Liquidite generale',
                 'Liquidite reduite',
                 'Taux ressources propres',
                 'Rentabilite des capitaux propres',
                 'Autonomie financiere',
                 'Poids interets',
                 'Taux EBE',
                 'Taux VA',
                 'Taux Rentabilite',
                 'Poids dettes fiscales',
                 'Tresorerie',
                 'Taux augmentation endettement CT',
                 'Croissance CA',
                 'Apport en comptes courants',
                 'Age entreprise']
    engine = sqlalchemy.create_engine(DATABASE_URI, echo=True)
    result = engine.execute("""SELECT * FROM dataset;""")
    result_list = [r for r in result]
    df_dataset = pd.DataFrame(result_list, columns=dataset_cols)
    return df_dataset

def get_naf_db(DATABASE_URI = "postgresql://marie@localhost:5432/fail_prediction"):
    """Gets a df holding mapping table to convert sectors codes into différent granularity levels
    from a PostrgreSQL database

    Args:
        DATABASE_URI (str, optional): URI of database with format as follow : f"postgresql://{DB_USERNAME}@{DB_ADDRESS}:{DB_PORT}/{DB_NAME}"
        Defaults to "postgresql://marie@localhost:5432/fail_prediction".

    Returns:
        pandas.DataFrame: df holding mapping table to convert sectors codes into différent granularity levels
    """
    engine = sqlalchemy.create_engine(DATABASE_URI, echo=True)
    res_naf = engine.execute("SELECT * FROM NAF")
    naf_list = [r for r in res_naf]
    df_naf_db = pd.DataFrame(naf_list, columns=res_naf.keys())
    return df_naf_db


