import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import MiniBatchKMeans
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple, Any
import multiprocessing as mp
from functools import partial
from src.analysis.repo.repo import AnalysisRepository
from src.storage.unit_of_work import UnitOfWork
import warnings

warnings.filterwarnings('ignore')


class TextBasedRepositoryClustering:
    def __init__(self, analysis_repo, n_workers: int = None):
        self.analysis_repo = analysis_repo
        self.n_workers = n_workers or mp.cpu_count()

        # Индикаторы для названий организаций (до слеша)
        self.corporate_indicators = {
            'microsoft', 'google', 'apple', 'amazon', 'meta', 'facebook', 'twitter',
            'netflix', 'uber', 'airbnb', 'spotify', 'slack', 'github', 'gitlab',
            'oracle', 'ibm', 'intel', 'amd', 'nvidia', 'docker', 'kubernetes',
            'apache', 'mongodb', 'redis', 'elastic', 'splunk', 'datadog',
            'cisco', 'mozilla', 'jetbrains', 'atlassian', 'salesforce', 'adobe'
        }

        # Корпоративные суффиксы для названий организаций
        self.corporate_suffixes = {
            'inc', 'ltd', 'corp', 'corporation', 'company', 'tech', 'technologies',
            'software', 'systems', 'solutions', 'enterprise', 'studio', 'labs',
            'holdings', 'group', 'llc', 'gmbh', 'limited', 'co', 'org'
        }

        # Корпоративные топики
        self.corporate_topics = {
            'api', 'sdk', 'framework', 'library', 'cloud', 'platform',
            'enterprise', 'production', 'backend', 'frontend', 'mobile',
            'web', 'database', 'server', 'client', 'toolkit', 'engine'
        }

        self.educational_indicators = {
            'course', 'tutorial', 'homework', 'assignment', 'student', 'lab',
            'exercise', 'project', 'university', 'college', 'school', 'learn',
            'training', 'education', 'academic', 'coursera', 'edx', 'udemy',
            'lesson', 'lecture', 'workshop', 'bootcamp', 'curriculum', 'syllabus',
            'exam', 'quiz', 'thesis', 'dissertation'
        }

        self.personal_indicators = {
            'my', 'personal', 'private', 'test', 'demo', 'example', 'sandbox',
            'playground', 'experiment', 'trial', 'prototype', 'mock', 'sample',
            'blog', 'portfolio', 'website', 'resume', 'cv'
        }

    def map_extract_textual_data(self, chunk: pd.DataFrame) -> List[Dict]:
        results = []

        for _, row in chunk.iterrows():
            repo_id = row['repo_id']
            full_name = str(row['full_name'] or '')
            description = str(row['description'] or '')
            topics = row['topics'] or []
            owner_login = str(row['owner_login'] or '')

            text_corpus = ' '.join([
                full_name,
                description,
                ' '.join(topics),
                owner_login
            ]).lower()

            org_name = self._extract_organization(full_name)

            corporate_weight = self._calculate_corporate_weight(full_name, description, topics, org_name)
            educational_weight = self._calculate_educational_weight(full_name, description, topics)
            personal_weight = self._calculate_personal_weight(full_name, description, topics)

            results.append({
                'repo_id': repo_id,
                'full_name': full_name,
                'text_corpus': text_corpus,
                'org_name': org_name,
                'corporate_weight': corporate_weight,
                'educational_weight': educational_weight,
                'personal_weight': personal_weight,
                'topics': topics,
                'owner_login': owner_login
            })

        return results

    def reduce_textual_features(self, mapped_results: List[List[Dict]]) -> pd.DataFrame:
        all_features = []
        for chunk_results in mapped_results:
            all_features.extend(chunk_results)

        return pd.DataFrame(all_features)

    def load_data_in_chunks(self, chunk_size: int = 1000) -> List[pd.DataFrame]:
        print("Loading data in chunks...")

        chunks = self.analysis_repo.load_repository_data(chunk_size)

        print(f"Loaded {len(chunks)} chunks, total {sum(len(chunk) for chunk in chunks)} repositories")
        return chunks

    def parallel_text_processing(self, chunks: List[pd.DataFrame]) -> pd.DataFrame:
        print("Parallel processing of textual features...")

        with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
            map_func = partial(self.map_extract_textual_data)
            mapped_results = list(executor.map(map_func, chunks))

        features_df = self.reduce_textual_features(mapped_results)

        print(f"Processed {len(features_df)} repositories")
        return features_df

    def _extract_organization(self, full_name: str) -> Optional[str]:
        if not isinstance(full_name, str) or '/' not in full_name:
            return None

        org = full_name.split('/')[0].lower().strip()

        personal_names = {
            'user', 'test', 'demo', 'example', 'temp', 'tmp', 'admin',
            'my', 'personal', 'private', 'backup', 'guest', 'anonymous'
        }

        if org in personal_names or len(org) < 2:
            return None

        return org

    def _calculate_corporate_weight(self, full_name: str, description: str, topics: List[str], org_name: str) -> float:
        weight = 0.0

        # 1. Проверяем название организации (до слеша)
        if org_name:
            org_lower = org_name.lower()

            # Проверяем прямые совпадения с компаниями
            for company in self.corporate_indicators:
                if company in org_lower:
                    weight += 3.0
                    break

            # Проверяем корпоративные суффиксы
            for suffix in self.corporate_suffixes:
                if org_lower.endswith(suffix) or f"-{suffix}" in org_lower:
                    weight += 2.0
                    break

        # 2. Проверяем топики
        topic_weight = 0.0
        for topic in topics:
            topic_lower = topic.lower()
            for corp_topic in self.corporate_topics:
                if corp_topic in topic_lower:
                    topic_weight += 0.5

        weight += min(topic_weight, 2.0)

        # 3. Проверяем описание (меньший вес)
        description_lower = description.lower()
        desc_corp_indicators = 0
        for indicator in self.corporate_indicators:
            if indicator in description_lower:
                desc_corp_indicators += 1

        weight += min(desc_corp_indicators * 0.5, 1.5)

        return min(weight, 10.0)

    def _calculate_educational_weight(self, full_name: str, description: str, topics: List[str]) -> float:
        weight = 0.0

        text_to_check = f"{full_name} {description} {' '.join(topics)}".lower()

        edu_matches = 0
        for indicator in self.educational_indicators:
            if indicator in text_to_check:
                edu_matches += 1

        if edu_matches >= 2:
            weight = edu_matches * 1.0
        elif edu_matches == 1:
            weight = 0.5

        return min(weight, 10.0)

    def _calculate_personal_weight(self, full_name: str, description: str, topics: List[str]) -> float:
        weight = 0.0

        text_to_check = f"{full_name} {description} {' '.join(topics)}".lower()

        personal_matches = 0
        for indicator in self.personal_indicators:
            if indicator in text_to_check:
                personal_matches += 1

        if '/' not in full_name:
            weight += 2.0

        weight += personal_matches * 1.0

        corp_weight = self._calculate_corporate_weight(full_name, description, topics,
                                                       self._extract_organization(full_name))
        edu_weight = self._calculate_educational_weight(full_name, description, topics)

        if corp_weight < 1.0 and edu_weight < 1.0:
            weight += 2.0

        return min(weight, 10.0)

    def create_feature_matrix(self, features_df: pd.DataFrame) -> Tuple[Any, List]:
        print("Creating feature matrix...")

        text_corpus = features_df['text_corpus'].tolist()

        vectorizer = TfidfVectorizer(
            max_features=1000,
            min_df=2,
            max_df=0.8,
            stop_words='english',
            ngram_range=(1, 2),
            lowercase=True,
            use_idf=True
        )

        tfidf_matrix = vectorizer.fit_transform(text_corpus)

        weight_features = features_df[['corporate_weight', 'educational_weight', 'personal_weight']].values
        feature_matrix = np.hstack([tfidf_matrix.toarray(), weight_features])

        print(f"Created feature matrix: {feature_matrix.shape}")
        return feature_matrix, features_df['repo_id'].tolist()

    def perform_clustering(self, feature_matrix: np.ndarray, n_clusters: int = 3) -> np.ndarray:
        print("Performing clustering...")

        clustering = MiniBatchKMeans(
            n_clusters=n_clusters,
            random_state=42,
            batch_size=1000,
            max_iter=100
        )

        clusters = clustering.fit_predict(feature_matrix)

        print(f"Created {n_clusters} clusters")
        return clusters

    def assign_repository_types(self, features_df: pd.DataFrame, clusters: np.ndarray) -> pd.DataFrame:
        print("Assigning repository types...")

        features_df = features_df.copy()
        features_df['cluster'] = clusters

        def determine_individual_type(row):
            weights = {
                'corporate': row['corporate_weight'],
                'educational': row['educational_weight'],
                'personal': row['personal_weight']
            }

            max_type = max(weights.items(), key=lambda x: x[1])[0]
            max_weight = weights[max_type]

            if max_weight < 1.0:
                return 'personal'

            if max_type == 'corporate' and max_weight < 2.0:
                sorted_weights = sorted(weights.items(), key=lambda x: x[1], reverse=True)
                if len(sorted_weights) > 1 and sorted_weights[1][1] > 0.5:
                    return sorted_weights[1][0]
                return 'personal'

            return max_type

        features_df['final_type'] = features_df.apply(determine_individual_type, axis=1)

        type_stats = features_df['final_type'].value_counts()
        print("Final type distribution:")
        for repo_type, count in type_stats.items():
            percentage = (count / len(features_df)) * 100
            print(f"  {repo_type}: {count} repositories ({percentage:.1f}%)")

        for repo_type in ['corporate', 'educational', 'personal']:
            examples = features_df[features_df['final_type'] == repo_type].head(3)
            if not examples.empty:
                print(f"\n{repo_type.upper()} examples:")
                for _, example in examples.iterrows():
                    weights = f"corp={example['corporate_weight']:.1f}, edu={example['educational_weight']:.1f}, pers={example['personal_weight']:.1f}"
                    print(f"  - {example['full_name']} ({weights})")

        return features_df

    def _calculate_confidence_score(self, row) -> float:
        weights = {
            'corporate': row['corporate_weight'],
            'educational': row['educational_weight'],
            'personal': row['personal_weight']
        }

        max_weight = max(weights.values())
        total_weight = sum(weights.values())

        if total_weight == 0:
            return 0.0

        confidence = (max_weight / total_weight) * (max_weight / 10.0)
        return min(confidence, 1.0)

    def analyze_results(self, results_df: pd.DataFrame) -> Dict[str, Any]:
        print("Analyzing results...")

        type_distribution = results_df['final_type'].value_counts()
        cluster_distribution = results_df['cluster'].value_counts()

        type_stats = results_df.groupby('final_type').agg({
            'corporate_weight': 'mean',
            'educational_weight': 'mean',
            'personal_weight': 'mean',
            'repo_id': 'count'
        }).rename(columns={'repo_id': 'count'})

        analysis_results = {
            'type_distribution': type_distribution.to_dict(),
            'cluster_distribution': cluster_distribution.to_dict(),
            'type_stats': type_stats.to_dict(),
            'total_repositories': len(results_df),
            'clusters_count': len(cluster_distribution)
        }

        return analysis_results

    def run_mapreduce_analysis(self, n_clusters: int = 3, chunk_size: int = 1000) -> Dict[str, Any]:
        print("Starting MapReduce repository analysis...")
        print(f"Parameters: {self.n_workers} workers, {n_clusters} clusters")

        try:
            chunks = self.load_data_in_chunks(chunk_size)

            if not chunks:
                return {"error": "No data available for analysis"}

            features_df = self.parallel_text_processing(chunks)

            print("\nDetailed weight analysis for sample repositories:")
            sample = features_df[['full_name', 'corporate_weight', 'educational_weight', 'personal_weight']].head(10)
            for _, row in sample.iterrows():
                org_name = self._extract_organization(row['full_name'])
                print(f"  {row['full_name']}")
                print(
                    f"    Org: {org_name}, Weights: corp={row['corporate_weight']:.1f}, edu={row['educational_weight']:.1f}, pers={row['personal_weight']:.1f}")

            feature_matrix, repo_ids = self.create_feature_matrix(features_df)
            clusters = self.perform_clustering(feature_matrix, n_clusters)
            results_df = self.assign_repository_types(features_df, clusters)

            results_df['confidence_score'] = results_df.apply(self._calculate_confidence_score, axis=1)

            analysis = self.analyze_results(results_df)

            analysis_summary = {
                'total_repositories': analysis['total_repositories'],
                'clusters_count': analysis['clusters_count'],
                'type_distribution': analysis['type_distribution'],
                'processing_stats': {
                    'chunks_processed': len(chunks),
                    'total_repositories': len(results_df),
                    'workers_used': self.n_workers
                },
                'algorithm_parameters': {
                    'n_clusters': n_clusters,
                    'chunk_size': chunk_size,
                    'n_workers': self.n_workers
                }
            }

            self.analysis_repo.save_clustering_results(results_df, analysis_summary)

            final_results = {
                'analysis': analysis,
                'feature_matrix_shape': feature_matrix.shape
            }

            self._print_results_summary(analysis)
            return final_results

        except Exception as e:
            print(f"Error during analysis: {e}")
            return {"error": str(e)}

    def _print_results_summary(self, analysis: Dict):
        print("\nCLUSTERING RESULTS SUMMARY")
        print("=" * 40)

        for repo_type, count in analysis['type_distribution'].items():
            percentage = (count / analysis['total_repositories']) * 100
            print(f"  {repo_type.upper():<12}: {count:>4} repositories ({percentage:>5.1f}%)")

        print(f"\nTotal processed: {analysis['total_repositories']} repositories")
        print(f"Clusters created: {analysis['clusters_count']}")


class RepositoryAnalyzer:
    def __init__(self, database_url: str, n_workers: int = None):
        UnitOfWork(database_url).create_analysis_tables()
        analysis_repo = AnalysisRepository(database_url)
        self.analyzer = TextBasedRepositoryClustering(analysis_repo, n_workers)

    def analyze(self, n_clusters: int = 3) -> Dict:
        return self.analyzer.run_mapreduce_analysis(n_clusters)