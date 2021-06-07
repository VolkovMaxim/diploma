import en_core_web_sm
import re
import nltk.data
nltk.download('punkt')
from tqdm.auto import tqdm
import itertools
from multiprocessing import Pool
import json
from tqdm import tqdm


def parse(path):
    g = open(path, 'rb')
    i = 0
    for l in tqdm(g):
        i += 1
        if 3_500_000 <= i < 6_500_000:
            yield json.loads(l)["text"]
        elif i >= 6_500_000:
            return []


def decontracted(phrase):
    # specific

    phrase = re.sub(r"won['’‘`]t", "will not", phrase)
    phrase = re.sub(r"can['’‘`]t", "can not", phrase)
    phrase = re.sub(r"ain['’‘`]t", "am not", phrase)

    # general
    phrase = re.sub(r"n['’‘`]t", " not", phrase)
    phrase = re.sub(r"['’‘`]re", " are", phrase)
    phrase = re.sub(r"['’‘`]s", " is", phrase)
    phrase = re.sub(r"['’‘`]d", " would", phrase)
    phrase = re.sub(r"['’‘`]ll", " will", phrase)
    phrase = re.sub(r"['’‘`]t", " not", phrase)
    phrase = re.sub(r"['’‘`]ve", " have", phrase)
    phrase = re.sub(r"['’‘`]m", " am", phrase)

    #phrase = re.sub('([.;!?])', r' \1 ', phrase)
    phrase = re.sub(r'[^\w.?!;]', ' ', phrase)
    phrase = re.sub(' +', ' ', phrase)
    sentences = tokenizer.tokenize(phrase)

    return sentences


def prepare_english_text(raw_text):
    preprocessed_texts = decontracted(raw_text)
    clean_sentences = []
    for preprocessed_text in preprocessed_texts:
        nlp_doc = nlp(preprocessed_text)
        clean_sentences.append(" ".join([token.ent_type_ or token.lemma_.lower() for token in nlp_doc]))
    return "\n".join(clean_sentences) + "\n"


def writer(texts, path):
    with open(path, "a", encoding="utf-8") as text_file:
        for text in texts:
            text_file.write(text)

tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')

nlp = en_core_web_sm.load()

if __name__ == '__main__':

    data = parse("yelp_academic_dataset_review.json")
    with Pool(8) as pool:
        chunksize = 1024
        for chunk in iter(lambda: list(itertools.islice(data, chunksize)), []):
            writer(pool.imap_unordered(prepare_english_text, chunk, chunksize=8), "clean_texts1.txt")

