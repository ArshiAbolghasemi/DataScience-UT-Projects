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

**Dimensionality Reduction and Visualization
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
