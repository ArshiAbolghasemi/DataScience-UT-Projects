## Introduction

This project explores advanced topics in modern data science through hands-on tasks that reflect real-world challenges. Spanning multiple domains—including natural language processing, computer vision, and semi-supervised learning—the goal is to build a deeper understanding of both traditional machine learning techniques and the capabilities of foundation models such as large language models (LLMs).

The project consists of four comprehensive tasks:

1. **Video Game Reviews** - Predicting review scores using limited labeled data and large amounts of unlabeled text, introducing semi-supervised and active learning strategies
2. **Semantic Search** - Building a Persian-language Q&A search engine powered by embeddings, vector databases, and reranking methods
3. **Complex Reasoning** - Working with the SWAG dataset using LLMs for multiple-choice reasoning through in-context learning, fine-tuning, and prompt engineering
4. **Image Segmentation** - Applying unsupervised clustering methods for image segmentation with evaluation using IoU and Dice coefficient metrics

## Task 1: Video Game Reviews

### Overview

The rise of online platforms has caused a huge increase in user-created content, like video game reviews. These reviews usually include short written summaries and a score from 1 to 10. While it's easy to collect the written reviews, getting accurate human-assigned scores for each one takes a lot of time and effort. This project focuses on predicting review scores when there aren't many labeled examples available, exploring different machine learning methods, including semi-supervised learning (SSL).

### Objective

Build accurate prediction models that use a small amount of labeled data along with a large amount of unlabeled data, reducing the need for extensive manual annotation work.

### Dataset Description

The project utilizes two distinct datasets, simulating a realistic scenario with limited annotation budget:

- **Labeled Dataset** (`labeled_reviews.csv`): Contains `review_text` and `review_score` (1-10). This dataset represents scarce, high-quality human-annotated data available for initial model training and validation.
- **Unlabeled Dataset** (`unlabeled_reviews.csv`): Contains only `review_text`. This larger pool of data is available without associated scores and will be leveraged by semi-supervised and active learning techniques.

The deliberate imbalance in dataset sizes (small labeled, large unlabeled) is central to evaluating the efficacy of techniques designed for low-resource environments.

### Task Structure

#### 1. Text Vectorization

Transform raw textual data into numerical representations suitable for machine learning algorithms.

**SentenceTransformer (Semantic Embeddings)**
- Use `sentence-transformers` library
- Load pre-trained model (`all-MiniLM-L6-v2`)
- Compute embeddings for all summaries in both datasets

**Word2Vec (Distributed Word Representations)**
- Use Gensim library to train Word2Vec model
- Train on combined corpus of all review summaries
- Compute sentence embeddings by averaging word vectors

**Dimensionality Reduction and Visualization**
- Perform PCA on generated embeddings
- Create scatter plots with color-coded scores
- Analyze clustering patterns and explain observations

#### 2. Supervised Learning Baseline

Establish baseline performance using only labeled data with two modeling paradigms:

**Classification Paradigm**
- Treat each score (1-10) as distinct categorical class
- Train classifiers (Logistic Regression, Random Forest, SVM, Ordinal Logistic Regression)

**Regression Paradigm**
- Treat score as continuous numerical value
- Train regressors (Linear Regression, SVR, Random Forest)

#### 3. Semi-Supervised Learning (SSL) Strategies
Leverage the large pool of unlabeled data to improve model performance.

**3.1 Pseudo-Labeling**
- Iterative SSL approach using high-confidence predictions
- Key concepts: confidence threshold, iterative refinement, confirmation bias risk

**3.2 Active Learning**
- Strategic selection of most informative samples for annotation
- Query strategies:
  - Least Confidence Sampling
  - Margin Sampling
  - Entropy-Based Sampling

## Task 2: Semantic Search on NiniSite

### Overview

This task involves developing a semantic search system for the PerCQA dataset, which contains approximately 1,000 Persian-language questions and over 21,000 answers from the NiniSite Q&A forum. The project focuses on building a comprehensive pipeline from data preprocessing to advanced semantic retrieval using modern embedding techniques and vector databases.

### Objective

Create a robust semantic search system that can retrieve and rank relevant answers based on user queries, moving beyond traditional keyword-based search to capture semantic meaning and context in Persian text.

### Dataset Description

The PerCQA dataset represents a real-world Persian Q&A forum with:
- **Questions**: ~1,000 Persian-language questions with metadata
- **Answers**: Over 21,000 community-generated answers
- **Challenges**: Informal Persian text with inconsistent formatting, Arabic character mixing, and social media-style expressions

### Task Structure

#### 1. Preprocessing

**Character Normalization**
- Normalize Persian and Arabic characters (e.g., "ي" → "ی", "ك" → "ک")
- Use tools like hazm or parsivar
- Handle inconsistent punctuation and unusual symbols

**Diacritics Removal**
- Remove Persian/Arabic diacritics ("ً", "ِ", "ّ", "َ", "ُ") 
- Simplify text for NLP processing
- Explore dataset for additional diacritical marks

**Tokenization**
- Break text into meaningful units (words/sentences)
- Use hazm, parsivar, or custom tokenization methods
- Handle Persian-specific tokenization challenges

**Stopword Removal**
- Remove common Persian stopwords ("از", "به", "که", "برای")
- Use hazm built-in lists or create custom stopword collections
- Focus models on meaningful content words

**Stemming and Lemmatization**
- Reduce word variations to base forms
- Compare stemming (root extraction) vs lemmatization (dictionary forms)
- Use hazm.Stemmer() and hazm.Lemmatizer()

**Informal Text Normalization**
- Handle letter repetition and stretching ("عااااالیییی" → "عالی")
- Apply regular expressions or normalization tools
- Reduce vocabulary complexity from casual writing

**Slang Replacement**
- Replace informal expressions ("خخخ", "عهههه") with standard equivalents
- using [Persian Informal Slang](./semantic_search/data/persian_informal_slang.json) file
- Improve model accuracy on social media text

#### 2. Exploratory Data Analysis

**Dataset Structure Analysis**
- Display sample questions with corresponding answers
- Compute length statistics (word count, character count)
- Create histograms and boxplots for length distributions

**Engagement Pattern Analysis**
- Identify questions with highest answer counts
- Analyze response rate patterns
- Understand community engagement dynamics

**User Activity Patterns**
- Analyze temporal patterns using CDate field
- Identify peak activity hours and days
- Create heatmaps and time-series visualizations

**Top Contributors Analysis**
- Count answers per user (CUsername)
- Visualize top contributors with bar charts
- Understand community participation patterns

**Linguistic Analysis**
- Extract frequent words from questions and answers
- Generate word clouds for pattern visualization
- Perform n-gram analysis (unigrams, bigrams, trigrams)
- Compare patterns before/after stopword removal

#### 3. Semantic Search Implementation

**Embedding Model Setup**
- Load and test bge-m3 multilingual embedding model
- Analyze model output components and their meanings
- Understand dense vs sparse embeddings

**Vector Database Configuration**
- Install and configure LanceDB
- Create TextEmbeddingFunction using bge-m3
- Focus on dense embeddings for semantic similarity

**Database Schema Design**
- Define schema with qid, qbody, and embedding fields
- Ensure proper data types and indexing
- Plan for efficient retrieval operations

**Data Population**
- Create and populate LanceDB table
- Automatic embedding generation during insertion
- Handle questions-only dataset (excluding comments)

**Semantic Search Implementation**
- Implement semantic search using LanceDB
- Retrieve top 5 results for multiple test queries
- Manual evaluation of semantic relevance

**Full-Text Search Comparison**
- Implement classical full-text search indexing
- Compare semantic vs keyword-based results
- Analyze strengths and weaknesses of each approach

**Hybrid Search Research**
- Investigate hybrid search methodologies
- Explain benefits of combining semantic and keyword search
- Discuss implementation strategies

**Evaluation Metrics**
- Research common search evaluation metrics
- Explain precision@k, recall, and NDCG
- Discuss manual vs automatic evaluation approaches

#### 4. Reranking Enhancement (10 pts bonus)

**Reranker Implementation**
- Apply bge-reranker or cross-encoder models
- Improve initial search result ordering
- Evaluate (question, answer) pair relevance

**Performance Comparison**
- Compare results before and after reranking
- Quantify improvements in result quality
- Analyze computational trade-offs
