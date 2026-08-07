import re

import json


from mrjob.job import MRJob
from mrjob.step import MRStep

class WordCounter(MRJob):

    def mapper(self, key, value):
        #read each json line
        review = json.loads(value)
        #extract review text
        review_text = review['reviewText']
        # split into words
        tokens = re.findall(r"\b\w+\b", review_text.lower())
        # Emit (word,1)
        for token in tokens:
            yield token, 1

        # Local aggreagation before reducer
        def combiner(self, key, values):
            yield key, sum(values)

        #count total
        def reducer(self,key,values):
            yield key, sum(values)

        def steps(self):
            return [
                MRStep(mapper=self.mapper,
                       combiner=self.combiner,
                       reducer=self.reducer)
            ]

if __name__ == '__main__':
    WordCounter.run()








