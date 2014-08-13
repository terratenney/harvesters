__author__ = 'mtenney'
import csv
class SimpleGraph:
    """
    Each of the three indexes holds a different permutation of each triple that is stored in
the graph. The name of the index indicates the ordering of the terms in the index (i.e., the
pos index stores the predicate, then the object, and then the subject, in that order).
The index is structured using a dictionary containing dictionaries that in turn contain
sets, with the first dictionary keyed off of the first term, the second dictionary keyed
off of the second term, and the set containing the third terms for the index. For example,
the pos index could be instantiated with a new triple like so:

    self._pos = {predicate:{object:set([subject])}}

A query for all triples with a specific predicate and object could be answered like so:
for subject in:

    self._pos[predicate][object]: yield (subject, predicate, object)

Each triple is represented in each index using a different permutation, and this allows
any query across the triples to be answered simply by iterating over a single index.
    """
    def __init__(self):
        self._spo = {}
        self._pos = {}
        self._osp = {}

    def add(self, (sub, pred, obj)):
        self._addToIndex(self._spo, sub, pred, obj)
        self._addToIndex(self._pos, pred, obj, sub)
        self._addToIndex(self._osp, obj, sub, pred)

    def _addToIndex(self, index, a, b, c):
        if a not in index:
            index[a] = {b:set([c])}
        else:
            if b not in index[a]:
                index[a][b] = set([c])
            else:
                index[a][b].add(c)

    def remove(self, (sub, pred, obj)):
        triples = list(self.triples((sub, pred, obj)))
        for (delSub, delPred, delObj) in triples:
            self._removeFromIndex(self._spo, delSub, delPred, delObj)
            self._removeFromIndex(self._pos, delPred, delObj, delSub)
            self._removeFromIndex(self._osp, delObj, delSub, delPred)

     def _removeFromIndex(self, index, a, b, c):
        try:
            bs = index[a]
            cset = bs[b]
            cset.remove(c)
            if len(cset) == 0:
                del bs[b]
            if len(bs) == 0:
                del index[a]
            # KeyErrors occur if a term was missing, which means that it wasn't a
            # valid delete:
        except KeyError:
            pass
     def load(self, filename):
        f = open(filename, "rb")
        reader = csv.reader(f)
        for sub, pred, obj in reader:
            sub = unicode(sub, "UTF-8")
            pred = unicode(pred, "UTF-8")
            obj = unicode(obj, "UTF-8")
            self.add((sub, pred, obj))
        f.close()
     def save(self, filename):
        f = open(filename, "wb")
        writer = csv.writer(f)
        for sub, pred, obj in self.triples((None, None, None)):
            writer.writerow([sub.encode("UTF-8"), pred.encode("UTF-8"), \
            obj.encode("UTF-8")])
        f.close()
     def triples(self, (sub, pred, obj)):
# check which terms are present in order to use the correct index:
        try:
            if sub != None:
                if pred != None:
                # sub pred obj
                    if obj != None:
                        if obj in self._spo[sub][pred]:
                            yield (sub, pred, obj)
                        # sub pred None
                        else:
                            for retObj in self._spo[sub][pred]:
                                yield (sub, pred, retObj)
                    else:
                        # sub None obj
                        if obj != None:
                            for retPred in self._osp[obj][sub]:
                                yield (sub, retPred, obj)
            # sub None None
                        else:
                            for retPred, objSet in self._spo[sub].items():
                                for retObj in objSet:
                                    yield (sub, retPred, retObj)
                else:
if pred != None:
# None pred obj
if obj != None:
for retSub in self._pos[pred][obj]:
yield (retSub, pred, obj)
# None pred None
else:
for retObj, subSet in self._pos[pred].items():
for retSub in subSet:
yield (retSub, pred, retObj)
else:
# None None obj
if obj != None:
