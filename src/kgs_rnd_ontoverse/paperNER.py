"""
This script performs a pairwise comparison of papers by CUI overlap:
  Input - the cuiAnnotationsCombined.pickle file created by the biomedical-NLP script (scispacyRun.ipynb)
  Output - the scoringdict.pickle file containing pairwise scores cut off at the user-specified value.
"""
import time
import logging
import math
import os
import pickle as pk

import pandas as pd
import spacy
from scispacy.abbreviation import AbbreviationDetector  # noqa: F401
from scispacy.linking import EntityLinker  # noqa: F401
from spacy.language import Language
from tqdm import tqdm

from kgs_rnd_ontoverse.utils.models import BibliographicObject

SAB_CONSO_FILES = [
    "pipeline_data/umls_data/cui2MSH.txt",
    "pipeline_data/umls_data/cui2HPO.txt",
    "pipeline_data/umls_data/cui2HGNC.txt",
    "pipeline_data/umls_data/cui2NCI.txt",
    "pipeline_data/umls_data/cui2RXNORM.txt",
]

# Configure logger
logger = logging.getLogger(__name__)


class OntoverseNERPipeline:
    """
    Class to handle the Ontoverse Named Entity Recognition pipeline.
    """

    def __init__(self, pipeline_data_path: str = "pipeline_data/", overwrite: bool = False) -> None:
        """
        Initialize the Ontoverse Named Entity Recognition pipeline.
        :param logging.Logger logger: logger instance for logging pipeline events
        :param str logging_level: logging level for pipeline events
        """
        # logger.setLevel(logging_level)
        logger.info("Initiating Ontoverse Named Entity Recognition pipeline")
        self.model_names = [
            "en_ner_craft_md",
            "en_ner_jnlpba_md",
            "en_ner_bc5cdr_md",
            "en_ner_bionlp13cg_md",
            "en_core_sci_scibert",
        ]
        self.pipeline_data_path = pipeline_data_path
        self.pipeline_umls_data_path = "pipeline_data/umls_data/"
        self.ontoverse_library_file = f"{pipeline_data_path}/ontoverse_library.pk"
        self.combined_cui_annotations_file = f"{pipeline_data_path}/cuiAnnotationsCombined.pk"
        self.overwrite = overwrite
        # Lazy-load models
        self.nlp_models_dict = {}
        self.ontoverse_library = None
        self.total_paper_no = 0
        self.top_paper_CUIs = None
        self.count_CUIs = None
        self.annotations = {}
        self.combined_annotations = {}

    def run(self) -> None:
        """
        Execute the Ontoverse NER pipeline.
        """
        logger.info("Starting Ontoverse NER pipeline")
        try:
            self.ontoverse_library = self.import_papers()
            self.annotations = self.run_annotations()
            self.integrate_annotations()
            self.restrict_annotations_to_present_CUIs()
            self.fetch_metadata()
            self.find_mappings()
            logger.info("End of NER pipeline")
        except Exception as e:
            logger.critical(f"Pipeline execution failed: {e}")
            raise

    def get_query_text(self, item_id: str) -> str:
        """
        Extract query text from the title and abstract of a paper.
        :param str item_id: unique identifier for a paper
        :return str: concatenated title and abstract
        """
        title = self.ontoverse_library[item_id].attributes.get("title", "")
        abstract = self.ontoverse_library[item_id].attributes.get("abstract", "")
        return f"{title} {abstract}".strip()

    def import_papers(self) -> dict[str, BibliographicObject]:
        """
        Import all papers to retrieve titles and abstracts for entity recognition.
        :return dict: dictionary of papers and their attributes
        """
        logger.info("Importing the papers from the library")
        try:
            with open(self.ontoverse_library_file, "rb") as handle:
                ontoverse_library = pk.load(handle)
            self.total_paper_no = len(ontoverse_library)
            logger.info(f"There are {self.total_paper_no} papers in the library")
            return ontoverse_library
        except FileNotFoundError as e:
            logger.error(f"Library file not found: {e}")
            raise

    def add_pipeline(self, nlp_model: Language) -> Language:
        """
        Add abbreviation detection and entity linking to the NLP pipeline.
        :param Language nlp_model: SpaCy NLP model instance
        :return Language: modified NLP model with additional components
        """
        nlp_model.add_pipe("abbreviation_detector")
        nlp_model.add_pipe("scispacy_linker", config={"resolve_abbreviations": True, "linker_name": "umls"})
        return nlp_model

    def run_annotations(self) -> dict[str, list[str]]:
        """
        Run named entity recognition and annotation on the papers using multiple models.
        :return dict: dictionary of papers with CUI annotations
        """
        logger.info("Start running annotations")
        annotations = {}
        for model in self.model_names:
            
            # try:
            output_file_path = f"{self.pipeline_data_path}/cuiAnnotations_{model}.tsv"

            if not self.overwrite:
                file_exists = os.path.isfile(output_file_path)
                if file_exists:
                    logger.info(f"Annotated file already exists for model {model}: {output_file_path}")
                    continue

            if model not in self.nlp_models_dict:
                self.nlp_models_dict[model] = spacy.load(model)
            nlp = self.add_pipeline(self.nlp_models_dict[model])
            # open the file for the current language model
            with open(output_file_path, "w") as fh:
                logger.info(f"Annotating with {model} will be saved at {output_file_path}")

                # Iterate through each paper and prepare the text for NLP,
                # NB that not all entries have both title and abstract
                start = time.time()
                for item_id in tqdm(self.ontoverse_library.keys(), desc=f"Annotating with {model}"):

                    if ("title", "abstract") in self.ontoverse_library[item_id].attributes:
                        query_text = (
                            self.ontoverse_library[item_id].attributes["title"]
                            + " "
                            + self.ontoverse_library[item_id].attributes["abstract"]
                        )
                    elif "title" in self.ontoverse_library[item_id].attributes:
                        query_text = self.ontoverse_library[item_id].attributes["title"]
                    elif "abstract" in self.ontoverse_library[item_id].attributes:
                        query_text = self.ontoverse_library[item_id].attributes["abstract"]
                    else:
                        # TODO: technically should probably break here?
                        continue
                        # perform nlp
                    doc = nlp(query_text)
                    if not doc.ents:
                        continue
                    entity = doc.ents[0]
                    # perform UMLS entity linking
                    # collate the CUIs
                    currentCUI = []
                    for umls_ent in entity._.kb_ents:
                        currentCUI.append(umls_ent[0])
                        # print(umls_ent[0])
                        # NB there's a lot of other meta-data in the linker well worth looking at later
                    outputAnnotationData = str(item_id) + "\t" + ",".join(currentCUI) + "\n"
                    fh.write(outputAnnotationData)
                    annotations[item_id] = currentCUI

                # end time
                end = time.time()
                time_taken = end - start
                logger.info(f"{model} took {time_taken/60} minutes to annotate {self.total_paper_no} papers")

        return annotations

    def integrate_annotations(self) -> None:
        """
        Integrate CUI annotations across multiple models into a single dictionary.
        """
        logger.info("Start integrating annotations")
        for model_name in self.model_names:
            filename = f"{self.pipeline_data_path}/cuiAnnotations_{model_name}.tsv"
            self.integrate_CUIs(filename)

        annotated_paper_no = len(self.combined_annotations.keys())
        logger.info(f"A total of {annotated_paper_no}/{self.total_paper_no} papers have annotations")

        with open(self.combined_cui_annotations_file, "wb") as fh:
            pk.dump(self.combined_annotations, fh)

    def integrate_CUIs(self, filename: str) -> dict[str, list[str]]:
        """
        Integrate CUI lists from different files into a single dictionary.
        :param str filename: file containing CUI annotations
        :return dict: updated dictionary of combined annotations
        """
        logger.info(f"Start CUIs integration for file {filename}")
        try:
            with open(filename) as input_file:
                for line in input_file:
                    line = line.strip()
                    if line:
                        try:
                            item_id, CUIs = line.split("\t", 1)
                            cui_list = [cui.strip() for cui in CUIs.split(",") if cui.strip()]
                            self.combined_annotations.setdefault(item_id, [])
                            self.combined_annotations[item_id] = list(
                                set(self.combined_annotations[item_id] + cui_list)
                            )
                        except ValueError:
                            logger.debug(f"Incorrectly formatted line in {filename}: {line}")
        except FileNotFoundError as e:
            logger.error(f"File {filename} not found during CUI integration: {e}")
            raise

        return self.combined_annotations

    def count_CUIs_ocurrences(self) -> dict[str, int]:
        """
        Count occurrences of each CUI across all papers.
        :return dict: dictionary with CUI counts
        """
        count_CUIs = {}
        for _paper_id, cui_list in self.combined_annotations.items():
            for cui in cui_list:
                count_CUIs[cui] = count_CUIs.get(cui, 0) + 1
        logger.info(f"There are {len(count_CUIs)} unique CUIs")
        return count_CUIs

    def restrict_annotations_to_present_CUIs(self) -> dict[str, list[str]]:
        """
        Restrict annotations to CUIs present in selected UMLS sources.
        :return dict: dictionary of top CUI annotations for each paper
        """
        count_CUIs = self.count_CUIs_ocurrences()
        restricted_CUIs = self.restricted_SAB_CUIs(SAB_CONSO_FILES)
        annotation_CUI_list = list(count_CUIs.keys())
        restricted_CUIs = list(set(annotation_CUI_list).intersection(set(restricted_CUIs)))
        restricted_CUI_counts = {key: count_CUIs[key] for key in restricted_CUIs}
        total_paper_no = len(self.combined_annotations)
        top_paper_CUIs = {}

        for paper_id, current_CUI_list in self.combined_annotations.items():
            current_paper_TFIDFs = {
                cui: math.log10(total_paper_no / restricted_CUI_counts[cui])
                for cui in current_CUI_list
                if cui in restricted_CUI_counts
            }
            sorted_TFIDFs = dict(sorted(current_paper_TFIDFs.items(), key=lambda item: item[1], reverse=True))
            top_CUIs = list(sorted_TFIDFs)[:3]
            top_paper_CUIs[paper_id] = top_CUIs
            logger.debug(f"Top CUIs for paper <{paper_id}> are <{top_CUIs}>")
        self.count_CUIs = count_CUIs
        self.top_paper_CUIs = top_paper_CUIs
        return top_paper_CUIs

    def restricted_SAB_CUIs(self, SAB_conso_files: list[str]) -> list[str]:
        """
        Filter CUIs based on SABs of interest.
        :param list SAB_conso_files: list of UMLS source files of interest
        :return list: list of unique CUIs associated with specific SABs
        """
        restricted_SABCUI_list = []
        for current_SAB_conso_file in SAB_conso_files:
            # current_SAB_conso_file = f"{self.pipeline_umls_data_path}/{current_SAB_conso_file}"
            current_SAB = pd.read_csv(current_SAB_conso_file, sep="|", header=None, low_memory=False)
            current_SAB.columns = [
                "CUI",
                "LAT",
                "TS",
                "LUI",
                "STT",
                "SUI",
                "ISPREF",
                "AUI",
                "SAUI",
                "SCUI",
                "SDUI",
                "SAB",
                "TTY",
                "CODE",
                "STR",
                "SRL",
                "SUPPRESS",
                "CVF",
                "TEST",
            ]
            restricted_SABCUI_list += list(current_SAB["CUI"].drop_duplicates())
        return list(set(restricted_SABCUI_list))

    def fetch_metadata(self) -> None:
        """
        Fetch metadata for the CUIs that have been annotated to library papers.
        """
        logger.info("Start fetching metadata for CUIs")
        self.MeSH_CUIMetaData = self.cui_metadata_mapper(
            f"{self.pipeline_umls_data_path}/cui2MSH.txt", self.count_CUIs, "MH"
        )  # noqa: E501
        self.HPO_CUIMetaData = self.cui_metadata_mapper(
            f"{self.pipeline_umls_data_path}/cui2HPO.txt", self.count_CUIs, "PT"
        )  # noqa: E501
        self.HGNC_CUIMetaData = self.cui_metadata_mapper(
            f"{self.pipeline_umls_data_path}/cui2HGNC.txt", self.count_CUIs, "PT"
        )  # noqa: E501
        self.NCI_CUIMetaData = self.cui_metadata_mapper(
            f"{self.pipeline_umls_data_path}/cui2NCI.txt", self.count_CUIs, "PT"
        )  # noqa: E501
        self.RxNORM_CUIMetaData = self.cui_metadata_mapper(
            f"{self.pipeline_umls_data_path}/cui2RXNORM.txt", self.count_CUIs, "IN"
        )

        logger.info(f"MeSH df shape: {self.MeSH_CUIMetaData.shape}")
        logger.info(f"HPO df shape: {self.HPO_CUIMetaData.shape}")
        logger.info(f"HGNC df shape: {self.HGNC_CUIMetaData.shape}")
        logger.info(f"NCI df shape: {self.NCI_CUIMetaData.shape}")
        logger.info(f"RxNORM df shape: {self.RxNORM_CUIMetaData.shape}")

    def cui_metadata_mapper(self, sab_conso_file: str, CUICounts: dict[str, int], tty_code: str) -> pd.DataFrame:
        """
        Map CUIs to metadata based on the UMLS source file.
        :param str sab_conso_file: UMLS source file containing CUI mappings
        :param dict CUICounts: dictionary of CUI counts
        :param str tty_code: UMLS code for the preferred name
        :return pd.DataFrame: DataFrame of CUI mappings with selected metadata
        """
        full_SAB_mapping = pd.read_csv(sab_conso_file, sep="|", header=None, low_memory=False)
        full_SAB_mapping.columns = [
            "CUI",
            "LAT",
            "TS",
            "LUI",
            "STT",
            "SUI",
            "ISPREF",
            "AUI",
            "SAUI",
            "SCUI",
            "SDUI",
            "SAB",
            "TTY",
            "CODE",
            "STR",
            "SRL",
            "SUPPRESS",
            "CVF",
            "TEST",
        ]
        paper_CUIs = pd.Series(list(CUICounts.values()), index=self.count_CUIs.keys(), name="cuiCounts")
        paper_CUI_meta = pd.merge(full_SAB_mapping, paper_CUIs, left_on="CUI", right_index=True)
        paper_CUI_meta = paper_CUI_meta.loc[(paper_CUI_meta["TTY"] == tty_code)]
        paper_CUI_meta = paper_CUI_meta[["CUI", "SAB", "CODE", "STR"]].drop_duplicates()
        return paper_CUI_meta

    def find_mappings(self) -> None:
        """
        Use top CUIs from each paper and finds the
        mappings of those CUIs with their associated terms
        (e.g.: CUI001 is Breast Cancer). Then, it creates keywords for each paper.
        """
        logger.info("Generating keywords for each paper")
        group = [
            self.MeSH_CUIMetaData,
            self.HPO_CUIMetaData,
            self.HGNC_CUIMetaData,
            self.NCI_CUIMetaData,
            self.RxNORM_CUIMetaData,
        ]
        master_CUI_meta_data = pd.concat(group)
        paper_keyword_data = {}
        for paper_id, current_CUIs in self.top_paper_CUIs.items():
            paper_keywords = master_CUI_meta_data[master_CUI_meta_data["CUI"].isin(current_CUIs)]
            paper_keyword_list = paper_keywords.to_csv(header=False, index=False).strip("\n").split("\n")
            paper_keyword_data[paper_id] = paper_keyword_list
        logger.info(f"There is a total of {len(paper_keyword_data)} keywords in {self.total_paper_no} papers")


if __name__ == "__main__":
    # Logger setup
    from setup_logger import setup_logging

    setup_logging()
    logger.info("Starting NER Pipeline")
    ontoverse_ner_pipeline = OntoverseNERPipeline(overwrite=False)
    ontoverse_ner_pipeline.run()
