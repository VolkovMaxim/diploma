from collections import Counter
from tqdm import tqdm

def ppmi_train(corpus, min_count, sample, window,
               context_alpha, negative, size, filter):
    vocab =
    cooccur = Counter()
    tokinizer = lambda x: x.split()
    for sent in tqdm(corpus, 44251363):
        sent = tokinizer(sent)



