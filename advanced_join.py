from pyspark import SparkContext
show_views_file = sc.textFile("input/join2_gennum?.txt")
show_views_file.take(2)
#[u'Hourly_Sports,21', u'PostModern_Show,38']

def split_show_views(line):
    value = line.split(",")
    show = value[0]
    views = value[1]
    return (show, views)
    
show_views = show_views_file.map(slit_show_views)    

show_channel_file = sc.textFile("input/join2_genchan?.txt")
show_channel_file.take(2)


def split_show_channel(line):
    value = line.split(",")
    show = value[0]
    channel = value[1]    
    return (show, channel)
    
show_channel = show_channel_file.map(split_show_channel)    

joined_dataset = show_channel.join(show_views)

def extract_channel_views(show_views_channel): 
    value = show_views_channel[1]
    channel = value[0]
    views = value[1] 
    return (channel, views)
    
def some_function(a, b):
    some_result = a + b
    return some_result    
    
channel_views.reduceByKey(some_function).collect()
