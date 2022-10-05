##### PREPARE INPUT DATA FOR API FROM SAMPLE COMPANIES #####
def select_sample():
    df2016 = pd.read_csv("../data/data_used/total.csv")
    df2018 = pd.read_csv("../data/data_used/total_2018.csv")
    def select_rows(row):
            if (row[0] not in siret2018 and row[48] != 0):
                return True
            else:
                return False
    df2016["test"] = df2016.apply(lambda row: select_rows(row), axis=1)
    sample = df2016[df2016["test"]]
    to_suppr_2016 = ['Catégorie juridique (Niveau I)', 'Effectif moyen du personnel (YP) 2016', 'Notation NOTA-PME 2016']
    leftover_to_suppr = ['SIREN',
     'Dénomination',
     'SIRET',
     'Procédures collectives (type)',
     'Procédures collectives (date)',
     'Code postal',
     'Tranche effectifs',
     'Emprunts remboursés en cours d’exercice (VK) 2016 (€)',
     "Emprunts souscrits en cours d’exercice - à plus d'un an et 5 ans au plus (VJ3) 2016 (€)",
     'Emprunts souscrits en cours d’exercice - à plus de 5 ans (VJ4) 2016 (€)',
     'Ville',
     'default',
     'test']
    sample = sample.drop(to_suppr_2016, axis=1)
    sample = sample.drop(leftover_to_suppr, axis=1)
    return sample

def print_company_sample(df, row_index):
    decoding_dic2016 = {'A': 'Code APE',
     'B': 'Date de création',
     'C': 'Capital souscrit non appelé (I) (AA) 2016 (€)',
     'D': 'TOTAL (II) (net) (BJNET) 2016 (€)',
     'E': 'Matières premières, approvisionnements (net) (BLNET) 2016 (€)',
     'F': 'En cours de production de biens (net) (BNNET) 2016 (€)',
     'G': 'En cours de production de services (net) (BPNET) 2016 (€)',
     'H': 'Produits intermédiaires et finis (net) (BRNET) 2016 (€)',
     'I': 'Marchandises (net) (BTNET) 2016 (€)',
     'J': 'Avances et acomptes versés sur commandes (net) (BVNET) 2016 (€)',
     'BA': 'Clients et comptes rattachés (3) (net) (BXNET) 2016 (€)',
     'BB': 'Autres créances (3) (net) (BZNET) 2016 (€)',
     'BC': 'Capital souscrit et appelé, non versé (net) (CBNET) 2016 (€)',
     'BD': 'Valeurs mobilières de placement (net) (CDNET) 2016 (€)',
     'BE': 'Disponibilités (net) (CFNET) 2016 (€)',
     'BF': "Charges constatées d'avance (3) (net) (CHNET) 2016 (€)",
     'BG': 'TOTAL (III) (net) (CJNET) 2016 (€)',
     'BH': 'Primes de remboursement des obligations (CM) 2016 (€)',
     'BI': 'Ecarts de conversion actif (CN) 2016 (€)',
     'BJ': 'TOTAL GENERAL(I à VI) (net) (CONET) 2016 (€)',
     'CA': 'Capital social ou individuel (1) (DA) 2016 (€)',
     'CB': 'Report à nouveau (DH) 2016 (€)',
     'CC': "RESULTAT DE L'EXERCICE (bénéfice ou perte) (DI) 2016 (€)",
     'CD': 'TOTAL (I) (DL) 2016 (€)',
     'CE': 'TOTAL(II) (DO) 2016 (€)',
     'CF': 'TOTAL (III) (DR) 2016 (€)',
     'CG': 'Autres emprunts obligataires (DT) 2016 (€)',
     'CH': 'Emprunts obligataires convertibles (DS) 2016 (€)',
     'CI': 'Emprunts et dettes auprès des établissements de crédit (5) (DU) 2016 (€)',
     'CJ': 'Emprunts et dettes financières divers (DV) 2016 (€)',
     'DA': 'Avances et acomptes reçus sur commandes en cours (DW) 2016 (€)',
     'DB': 'Dettes fournisseurs et comptes rattachés (DX) 2016 (€)',
     'DC': 'Dettes fiscales et sociales (DY) 2016 (€)',
     'DD': 'Dettes sur immobilisations et comptes rattachés (DZ) 2016 (€)',
     'DE': 'Autres dettes (EA) 2016 (€)',
     'DF': "dont comptes courants d'associés de l'exercice N (EA2) 2016 (€)",
     'DG': "Produits constatés d'avance (EB) 2016 (€)",
     'DH': 'TOTAL (IV) (EC) 2016 (€)',
     'DI': 'TOTAL GENERAL (I à V) (EE) 2016 (€)',
     'DJ': '(5)\xa0Dont concours bancaires courants, et soldes créditeurs de banques et CCP (EH) 2016 (€)',
     'EA': "Chiffre d'affaires net (France) (FJ) 2016 (€)",
     'EB': "Chiffre d'affaires net (Exportations et livraisons intracommunautaires) (FK) 2016 (€)",
     'EC': "Chiffre d'affaires net (Total) (FL) 2016 (€)",
     'ED': "Subventions d'exploitation (FO) 2016 (€)",
     'EE': 'Reprises sur amortissements et provisions, transferts de charges (9) (FP) 2016 (€)',
     'EF': "Total des produits d'exploitation (2) (I) (FR) 2016 (€)",
     'EG': 'Achats de marchandises (y compris droits de douane) (FS) 2016 (€)',
     'EH': 'Variation de stock (marchandises) (FT) 2016 (€)',
     'EI': 'Achats de matières premières et autres approvisionnements (y compris droits de douane) (FU) 2016 (€)',
     'EJ': 'Variation de stock (matières premières et approvisionnements) (FV) 2016 (€)',
     'FA': 'Autres achats et charges externes (3) (6 bis) (FW) 2016 (€)',
     'FB': 'Impôts, taxes et versements assimilés (FX) 2016 (€)',
     'FC': 'Salaires et traitements (FY) 2016 (€)',
     'FD': 'Charges sociales (10) (FZ) 2016 (€)',
     'FE': "Dotations d'exploitation sur immobilisations (dotations aux amortissements) (GA) 2016 (€)",
     'FF': "Dotations d'exploitation sur immobilisations (dotations aux provisions) (GB) 2016 (€)",
     'FG': "Dotations d'exploitation sur actif circulant (dotations aux provisions) (GC) 2016 (€)",
     'FH': "Dotations d'exploitation pour risques et charges (dotations aux provisions) (GD) 2016 (€)",
     'FI': 'Autres charges (12) (GE) 2016 (€)',
     'FJ': "Total des charges d'exploitation (4) (II) (GF) 2016 (€)",
     'GA': "1 - RESULTAT D'EXPLOITATION (I - II) (GG) 2016 (€)",
     'GB': 'Total des produits financiers (V) (GP) 2016 (€)',
     'GC': 'Intérêts et charges assimilées (GR) 2016 (€)',
     'GD': 'Total des charges financières (VI) (GU) 2016 (€)',
     'GE': 'Dotations financières aux amortissements et provisions (GQ) 2016 (€)',
     'GF': 'Reprises sur provisions & transferts de charges (GM) 2016 (€)',
     'GG': 'Total des produits exceptionnels (VII) (HD) 2016 (€)',
     'GH': 'Reprises sur provisions & transferts de charges (HC) 2016 (€)',
     'GI': 'Dotations exceptionnelles aux amortissements et provisions (6 ter) (HG) 2016 (€)',
     'GJ': 'Total des charges exceptionnelles (VIII) (HH) 2016 (€)',
     'HA': '4 - RESULTAT EXCEPTIONNEL (VII - VIII) (HI) 2016 (€)',
     'HB': '5 - BENEFICE OU PERTE (Total des produits - total des charges) (HN) 2016 (€)',
     'HC': "Participation des salariés aux résultats de l'entreprise (HJ) 2016 (€)",
     'HD': 'Impôts sur les bénéfices (HK) 2016 (€)',
     'HE': 'Clients douteux ou litigieux - Montant brut (VA) 2016 (€)',
     'HF': 'Sécurité sociale et autres organismes sociaux - Montant brut (8D) 2016 (€)',
     'HG': 'Impôts sur les bénéfices - Montant brut (8E) 2016 (€)',
     'HH': 'T.V.A. - Montant brut (VW) 2016 (€)',
     'HI': 'Emprunts souscrits en cours d’exercice - à 1 an au plus (VJ2) 2016 (€)',
     'HJ': 'Effets portés à l’escompte et non échus (YS) 2016 (€)',
     'IA': 'Sous‐traitance (YT) 2016 (€)',
     'IB': 'Ecarts de conversion passif (V) (ED) 2016 (€)',
     'IC': 'Production stockée (FM) 2016 (€)',
     'ID': 'Production immobilisée (FN) 2016 (€)',
     'IE': '(3)\xa0Dont Crédit-bail mobilier (HP) 2016 (€)',
     'IF': '(3)\xa0Dont Crédit-bail immobilier (HQ) 2016 (€)',
     'IG': '3 - RESULTAT COURANT AVANT IMPOTS (I - II + III - IV + V - VI) (GW) 2016 (€)',
     'IH': "Nombre de mois de l'exercice comptable 2016",
     'II': 'Catégorie juridique (Niveau II)'}
    
    e1 = df.iloc[row_index]
    keys = list(decoding_dic2016.keys())
    sample_dic = {k:v for (k,v) in zip(keys, e1)}
    for k,v in sample_dic.items():
        print(f'"{k}": ', f'{v},')
    return sample_dic

def get_sample(row_index):
    df = select_sample()
    sample = print_company_sample(df, row_index)
    return sample