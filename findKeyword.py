#function to return ordered list of (keyword,score) from a given title
#  credit for this code goes to agiliq blog http://agiliq.com/blog/2009/03/finding-keywords-using-python/
#  some modifications implemented to ensure consistnecy for lower case and to use different data set
#  shakespeare data set sourced from http://norvig.com/ngrams/ under usage by MIT license 

def find_keyword(test_string = 'Hacker news is a good site while Techcrunch not so much'):
    test_string = test_string.lower() #ensure casing is consistent
    key_file = open('shakespeare.txt') #all words are lower case in this file
    data = key_file.read()
    words = data.split()
    word_freq = {}
    for word in words:
	word = word.lower() #ensures consistency
        if word in word_freq:
	    word_freq[word]+=1
        else:
            word_freq[word] = 1
    word_prob_dict = {}
    size_corpus = len(words)
    for word in word_freq:
        word_prob_dict[word] = float(word_freq[word])/size_corpus

    prob_list = []
    for word, prob in word_prob_dict.items():
         prob_list.append(prob)
    non_exist_prob = min(prob_list)/2

    words = test_string.split()
    test_word_freq = {}
    for word in words:
        if word in test_word_freq:
            test_word_freq[word]+=1
        else:
            test_word_freq[word] = 1

    test_words_ba = {}
    for word, freq in test_word_freq.items():
        if word in word_prob_dict:
            test_words_ba[word] = freq/word_prob_dict[word]
        else:
            test_words_ba[word] = freq/non_exist_prob

    test_word_ba_list = []
    for word, ba in test_words_ba.items():
         test_word_ba_list.append((word, ba))

    def sort_func(a, b):
        if a[1] > b[1]:
           return -1
        elif a[1] < b[1]:
           return 1
        
        return 0

    test_word_ba_list.sort(sort_func)
    return test_word_ba_list
