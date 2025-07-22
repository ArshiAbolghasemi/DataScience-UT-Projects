# Introduction

This project explores advanced topics in modern data science through hands-on tasks that reflect real-world challenges. Spanning multiple domains—including natural language processing, computer vision, and semi-supervised learning—the goal is to build a deeper understanding of both traditional machine learning techniques and the capabilities of foundation models such as large language models (LLMs).

The project consists of four comprehensive tasks:
1. **Video Game Reviews** - Predicting review scores using limited labeled data and large amounts of unlabeled text, introducing semi-supervised and active learning strategies
2. **Semantic Search** - Building a Persian-language Q&A search engine powered by embeddings, vector databases, and reranking methods
3. **LLM for Multiple Choice Questions** - Working with the SWAG dataset using LLMs for multiple-choice reasoning through in-context learning, fine-tuning, and prompt engineering
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

## Task 2: Semantic Search on NiniSite
### Overview
This task involves developing a semantic search system for the PerCQA dataset, which contains approximately 1,000 Persian-language questions and over 21,000 answers from the NiniSite Q&A forum. The project focuses on building a comprehensive pipeline from data preprocessing to advanced semantic retrieval using modern embedding techniques and vector databases.

### Objective
Create a robust semantic search system that can retrieve and rank relevant answers based on user queries, moving beyond traditional keyword-based search to capture semantic meaning and context in Persian text.

### Dataset Description
The PerCQA dataset represents a real-world Persian Q&A forum with:
- **Questions**: ~1,000 Persian-language questions with metadata
- **Answers**: Over 21,000 community-generated answers

## Task 3: LLM for Multiple Choice Questions
### Overview
In data science, not all problems fit traditional models like classification or regression. Tasks involving nuanced language or semantic understanding require more than pattern recognition. In such cases, large language models (LLMs), with their ability to grasp context and intent, are essential. This task explores how LLMs excel at complex inference beyond simple patterns, reinforcing the value of foundation models in practical data science applications.

### Objective
Understand the limits of traditional methods and the importance of semantic reasoning by working with the SWAG dataset to explore various LLM approaches including in-context learning, fine-tuning, and prompt engineering techniques.

### Dataset Description
The SWAG (Situations With Adversarial Generations) dataset contains over 113,000 multiple-choice questions based on real-world scenarios. Each example includes:
- **Context**: Situational setup (sent1 and sent2)
- **Multiple Endings**: Four candidate completions (ending0 to ending3)
- **Labels**: Correct answer indices for evaluation

The dataset is designed to evaluate contextual and commonsense reasoning abilities, making it ideal for testing LLM performance on complex inference tasks.

## Task 4: Football Image Segmentation
### Overview
Image segmentation is one of the most classic and extensively studied tasks in computer
vision, with a wide range of applications—from segmenting tumors in medical images to
identifying and separating different elements of a road scene in autonomous driving, such as
lanes, vehicles, pedestrians, and traffic signs. Despite this, image segmentation tasks often
face substantial challenges related to data. One of the primary issues is the need for
high-quality, pixel-level annotations, which are both time-consuming and expensive to
produce—especially in domains like medical imaging, where expert knowledge is required. To
address these challenges, unsupervised methods have been deployed to automatically
generate segmentation masks required for training these data. One such method is
clustering, which groups pixels based on their features—such as color, intensity, or spatial
information— into different segments. Although this might be a complex problem in cluttered
images it is possible for some image datasets.

### Dataset Description
In this section you are going to create segmentation masks for players from the [football player segmentation](https://www.kaggle.com/datasets/ihelon/football-player-segmentation) dataset.

