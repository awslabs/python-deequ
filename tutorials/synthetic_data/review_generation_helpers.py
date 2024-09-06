import numpy as np
import pandas as pd

# for review_id generation
import string
import random

# for review body and title generation
from essential_generators import DocumentGenerator

def generate_review_headline_body(n):
    """Generate review headlines and review bodies
    
    Args:
      n: integer, number of pairs to generate 

    Returns:
      pandas DataFrame with 2 columns: review_healine, review_body; number of rows = n
    
    """
    gen = DocumentGenerator()
    template = {'review_headline':'sentence', 
            'review_body': 'paragraph'}
    gen.set_template(template)
    documents = gen.documents(n)
    
    return pd.DataFrame(documents)


def create_customers_insight(random_number_generator, factor = 1):
    """Returns numpy array with the list of customers, reviews, insight flag.
    
    The function assumes a certain distribution: 1% of the customers wrote 100
    reviews, 1%: 50 reviews, 2%: 30 review, 10%: 15 reviews, 20%: 10 reviews,
    30%: 5 reviews, 30%: 3 reviews, 6%: 1 review.
    
    Review ids are unique.
    
    Insight (influencer flag, Y/N): customers with more reviews have a higher
    probability of being an insighter. The probability for Y are the following
    for the groups above: [0.99, 0.8, 0.4, 0.1, 0, 0, 0, 0] 
    
    To increase the number of reviews change the factor to the next integer. 

    Args:
      factor: value of 1 (default) will generate 3,224,806 review from 400,100
      customers. random_number_generator: random number generator object from
      numpy

    Returns:
      numpy array of the shape (2, number of reviews).
    """
    
    # define customer proportions, number of reviews from each group of customers, 
    # and the proportion of 'Y' for the insight
    distribution_parameters = np.array([[0.01, 0.01, 0.02, 0.1, 0.2, 0.3, 0.3, 0.06], 
                                        [100, 50, 30, 15, 10, 5, 3, 1],
                                        [0.99, 0.8, 0.4, 0.1, 0, 0, 0, 0]])
    
    total_customers = 400100*factor
    # customer_to_reviews_factor = np.multiply(distribution_parameters[0], 
    #                                          distribution_parameters[1]).sum()
    
    # calculate the number of customers in each review frequency group
    customers_per_group = distribution_parameters[0]*total_customers
    
    # create array indices to split customer_ids into cohorts
    split_indices = customers_per_group.cumsum()
    
    # generate customer ids:
    customer_ids = np.arange(100000, 100000 + total_customers)
    
    # create arrays with customer_ids: these will generate reviews at different
    # frequency as defined in the customer_review_insight[1]
    customer_cohorts = np.split(customer_ids, split_indices[0:-1].astype(int))
    
    # replicate customer_ids according to the number of reviews to generate
    # the array of the size of the reviews dataset
    customers_list = []
    for i in range(len(customer_cohorts)):
        customers_list.append(np.repeat(customer_cohorts[i], 
                                        distribution_parameters[1][i]))
    customers_array = np.hstack(customers_list)
    
    # generate insight vector which needs to be tightly associated with
    # customers
    insight_list = []
    for i in range(distribution_parameters.shape[1]):
        length_needed = (distribution_parameters[1][i]*customer_cohorts[i].shape[0]).astype(int)
        probability_needed = distribution_parameters[2][i]
        insight_list.append(random_number_generator.choice(['Y', 'N'], 
                                                     length_needed, 
                                                     p = [probability_needed, 
                                                          1 - probability_needed]))
    insight_array = np.hstack(insight_list)
    
    # combine customers and the insight flag into a single array:
    # it should be shuffled for later use
    cust_insight_combined = np.vstack([customers_array, insight_array])
    
    return cust_insight_combined


def shuffle_customer_insight(random_number_generator, customer_insight_array):
    """Shuffles the array of customers and the insight flag.
    
    Args:
      customer_insight_array: numpy array of the shape (2, number_of_reviews)

    Returns:
      numpy array of the shape (2, number_of_reviews) shuffled.
    
    """
    # create indices
    ind = np.arange(customer_insight_array.shape[1])
    # shuffled the indices
    random_number_generator.shuffle(ind)
    # subset the original array using randomized indices
    customer_insight_shuffled = customer_insight_array[:, ind]
    
    return customer_insight_shuffled


def subset_customer_data(random_number_generator, customer_insight_array):
    """Reduce the customer+insight array by a random number between 100 and 2000
    
    The goal is to introduce some variability to the datasets generated since
    the overall size is defined by function create_customers_insight and the
    associated factor.
    
    Args:
        customer_insight_array: numpy array of the shape (2, number_of_reviews)
        random_number_generator: random number generator object from numpy
    
    Returns:
        numpy array of the shape (2, number_of_reviews - (random number between
        100 and 2000))
    """

    rand_number = random_number_generator.integers(100, 2001)
    print(rand_number)
    customers_to_remove = random_number_generator.choice(np.arange(0,customer_insight_array.shape[1]), size = rand_number)
    return np.delete(customer_insight_array, customers_to_remove, axis = 1)
    

def generate_dates_even_per_month(random_number_generator, years, days_per_month):
    """ Generates review dates given a list of years

    This function was used to generate the same number of dates per months for the Jewelry 
    dataset where trends needed to be observed. 
    
    Args: 
        years: list of years for which to generate dates
        
        days_per_month: how many dates is needed for each month, int. The same number of random
        dates will be generated for each month
        
        random_number_generator: random number generator object from numpy
    
    Returns: list of dates in the natural order for month, the same order for years as in the input arg 'years'
    """
    output = []
    # source of days per each month
    month_days = {'01': 31, '02': 28, '03': 31, '04':30, '05':31, '06':30, '07':31, '08':31, '09':30, '10':31, '11': 30, '12': 31}
    for e in years:
        for m,d in month_days.items():
            # generate an array of days for a given month, convert to string
            d_range = np.arange(1, d+1).astype(str)
            # pad the array of days with 0s on the right to ensure correct date representation
            d_range_str = np.char.zfill(d_range, 2)
            # create an array of 'year-month-' strings
            year_month = np.repeat(str(e) + '-' + m + '-', d_range_str.shape[0])
            # concatenate the array of 'year-month-' strings with the days
            source_dates = np.char.add(year_month, d_range_str)
            # create a sample of dates for a given year/month, add to the output array
            output.extend(random_number_generator.choice(source_dates, size = days_per_month)) 
    return output


def generate_dates(random_number_generator, year_array):
    """Generate random dates for each instance of year in the input array
    
    The function will subset from all dates in 2023 (non-leap) in uniform fashion.
    
    Args:
        year_array: numpy array of years, shape is (number_of_reviews, ).
        random_number_generator: random number generator object from numpy.
        
    Returns: pandas DataFrame with 2 columns ("review_year" and "review_date" in ISO format) and 
    the number of rows equal to the size of the year_array input.
    """  
    # we will sample from a single non-leap year
    test_date1, test_date2 = np.datetime64('2023-01-01'), np.datetime64('2023-12-31')
    
    # calculate the difference in days
    dates_bet = test_date2 - test_date1
    
    # sample the required number of offsets, to be added to test_date1
    
    days_offset = random_number_generator.choice(dates_bet.astype(int), 
                                                 size = year_array.shape[0]).astype('timedelta64[D]')
    
    # create a new datetime object by adding the time delta
    start_date = np.repeat(test_date1, year_array.shape[0], axis = 0)
    random_dates = start_date + days_offset
    
    # extract month and day, assemble results to a data frame
    months = random_dates.astype('datetime64[M]').astype(int) % 12 + 1
    days = (random_dates - random_dates.astype('datetime64[M]') + 1).astype(int)
    
    # assemble into a data frame and join to get dates, it was easier to work with pandas than numpy 
    # in this case
    year_ser = pd.Series(year_array.astype(str))
    days_ser = pd.Series(days.astype(str)).str.pad(width = 2, side = 'left', fillchar = '0')
    month_ser = pd.Series(months.astype(str)).str.pad(width = 2, side = 'left', fillchar = '0')
    df = pd.concat([year_ser, month_ser, days_ser], axis = 1)
    df.columns = ["review_year", "month", "day"]
    df['review_date'] = df.agg('-'.join, axis=1)
    
    return df[["review_year", "review_date"]]


def generate_products(random_number_generator, product_components,  n):
    """Generate 2D array with product names and the associated product ids.
    
    The function generates a list of 10,000 random combinations of product
    names, product prefixes (color etc), product suffixes (to be added after
    product name and a comma). With the this pool it then generates the final
    list of n products and the product ids according to the exponential
    distribution (scale = 1). Product id is generated as a random integer from
    10,000 to 100,000.
    
     Args:
      random_number_generator: random number generator object from numpy.
      
      product_components: list of 1 or 3 lists
        If 3 lists are found we assume that the first one contains prefixes
        (such as color, material, make, etc.), the second list contains product
        names, and the third list contains sufffixes (such as 'with
        stones','with knobs', 'with extra features', 'perfect gift' etc). In
        this case we will generate new product names by creating random
        combinations between prefixes, names and suffixes. If 1 list is found
        we assume that the product titles already provided and we will only
        sample from them instead of creating random combinations. This
        parameterer is used to create product titles
      
      n: integer, final number of product names and product ids to generate.

    Returns:
      numpy array of the shape (2, n).
    
    """
    
    if len(product_components) == 3:
        # create a pool of 10000 products
        all_products = [''.join(random_number_generator.choice(product_components[0]) + 
                        ' ' + 
                        random_number_generator.choice(product_components[1]) + 
                        ', ' + 
                        random_number_generator.choice(product_components[2])) 
                        for x in range(10000)]
    elif len(product_components) == 1:
        all_products = random_number_generator.choice(product_components[0], 
                                                      size = 10000)
    else:
        raise ValueError("The length of product_components list must be either 3 or 1.")
    
    # create random ids to be associated with the products
    ids_pool = random_number_generator.choice(np.arange(10000,100000), 
                                              size = len(all_products))
    
    # combine products and the ids
    out = np.vstack((all_products, ids_pool),  dtype = str)
    
    # draw from the exponential distribution
    exp_weights = random_number_generator.exponential(scale = 1, 
                                                      size = len(all_products))
    
    # generate random indices according to the exponential distribution
    indx = random_number_generator.choice(out.shape[1], size = n,  
                                          p = exp_weights/exp_weights.sum())
    
    # create a random set of product names and product ids:
    product_titles_ids = out[:,indx]
    
    return product_titles_ids
    
    
def generate_random_review_id(n):
    """Generate n strings of length 15
    
    This simulates unique review ids. We generate n strings which start with R
    and end with a mix of uppercase letters and digits.
    
    Args:
        n: integer, total number of strings to generate
    
    Returns: list of n strings
    """
    
    return ['R' + 
            ''.join(
                random.choices(string.ascii_uppercase + string.digits, k=14)
                ) 
            for x in range(n)
            ]


def subset_array_exponential(random_number_generator, input_array, n, scale, sort = False):
    """Generate an array of size n from a smaller array

    Generation follows exponential distribution.
    
    Example: generate n star ratings where each star rating (1 to 5) is drawn
    from the shuffled exponential distribution
    
    Args:
        random_number_generator: random number generator object from numpy.

        input_array: numpy array with the desired values (such as star ratings,
        helpful votes, etc) 
        
        n: integer, length of the final array 
        
        scale: float. The scale parameter, inverse of the rate parameter, must be
            non-negative. Used in numpy.random.exponential()
        
        sort: bool. Whether to sort the exponential weight. If False then the weights
        are taken as is (random), if True then the weights are sorted in increasing
        order. Sorting is recommended when the input_array is years to mimic increasing
        number of reviews with time. 

        
        
    Returns: numpy array of shape (n,)
    """
    
    # draw from exponential distribution according to the size of the input array
    exp_weights = random_number_generator.exponential(scale = scale, 
                                                      size = input_array.shape[0])
    
    if sort:
        exp_weights.sort()
    
    # generate random indices according to the exponential distribution
    indx = random_number_generator.choice(np.arange(0, input_array.shape[0]), 
                                          size = n, 
                                          p = exp_weights/exp_weights.sum())
    # randomly subset the input array to the needed size, return
    return input_array[indx,]
                                   
    
def generate_marketplace_array(random_number_generator, n, scale, factor = 0.5):
    """Generate array size n of marketplace codes
    
    Args:
        random_number_generator: random number generator object from numpy. 
        
        n: integer, length of the final array 

        scale: float. The scale parameter, inverse of the rate parameter, must be
            non-negative.
        
        factor: float, portion of marketplaces to subset. 1 = 100%, 0.5 = 50%. 
        0.5 is the default
    
    Returns: numpy string array of shape (n, ) with marketplace codes generated
    according to exponential distribution.
    """
    mp_pool = np.array(['US', 'CA', 'MX', 'AR', 'AU', 'UK', 'BR', 'CN', 'CO', 'CR',
                       'FR', 'DE', 'HK', 'IN', 'IT', 'JP', 'PL', 'SG', 'ES', 'CH'], 
                       dtype = str)
    mp_selected = random_number_generator.choice(mp_pool, 
                                                 size = int(factor*mp_pool.shape[0]))
    
    return subset_array_exponential(random_number_generator, 
                                    mp_selected, 
                                    n, 
                                    scale = scale,
                                    sort = False)
    
    

def create_dataset(size_factor = 1, 
                   total_votes = np.arange(0, 50, 5),
                   helpful_votes = np.arange(0, 31), 
                   scale = 1, 
                   review_years = np.arange(1997, 2015), 
                   product_category = 'Electronics', 
                   product_components = [["pink"], ["stove"], ["with knobs"]],
                   marketplace_factor = 1):
    
    """Generate columns for reviews
    

    Args: size_factor: int
        when = 1 the initial number of reviews generated is 3,224,806 from
        400,100 customers. To increase the number of customers (and,
        correspondingly, reviews) use a different factor (2 for 400,100*2, 3 for
        400,100*3 etc). The final number of reviews will be reduced by a random
        number between 100 and 2000. This defines the size of the final dataset
    total_votes: numpy array
        used to select a random number to assign the number of total votes to
        each review. Can provide as numpy.arange()
    helpful_votes: numpy array
        used to select a random number to assign the number of helpful votes to
        each review. Can provide as numpy.arange()
    scale: float
        beta or scale parameter in numpy.random.exponential. Will be used for
        all data generators that rely on exponential distribution
    review_years: numpy array
        used to select years for reviews. Number of reviews per year follows
        exponential distribution 
    product_category: string
        name of the product category
    product_components: list of 1 or 3 lists
        If 3 lists are found we assume that the first one contains prefixes
        (such as color, material, make, etc.), the second list contains product
        names, and the third list contains sufffixes (such as 'with
        stones','with knobs', 'with extra features', 'perfect gift' etc). In
        this case we will generate new product names by creating random
        combinations between prefixes, names and suffixes. If 1 list is found
        we assume that the product titles already provided and we will only
        sample from them instead of creating random combinations. This
        parameterer is used to create product titles
    marketplace_factor: float
        used as % of marketplaces to use for the data. 1 will use 20
        marketplaces, 0.5 will use 10 marketplaces etc
    """
    
    rng = np.random.default_rng()
    
    # generate customer ids and the insight column:
    cust_ins = create_customers_insight(rng, factor = size_factor)

    # shuffle
    cust_ins_shuffle = shuffle_customer_insight(rng, cust_ins)

    # reduce the final size by a small random number, output: numpy (2, n)
    cust_ins_ready = subset_customer_data(rng, cust_ins_shuffle)

    # define the total number of rows in the final dataset
    n = cust_ins_ready.shape[1]

    # generate product titles and product ids, output: numpy array (2, n)
    products = generate_products(rng, product_components, n)

    # generate review identifiers, output: list of length n
    random.seed()
    review_id = generate_random_review_id(n)

    # generate star ratings, output: numpy array (n,)
    stars = np.array([1, 2, 3, 4, 5])
    star_rating = subset_array_exponential(rng, stars, n, 
                                           scale = scale, sort = False)

    # generate total votes, output: numpy array (n,)
    total_votes = subset_array_exponential(rng, total_votes, n, 
                                           scale = scale, sort = False)

    # generate helpful votes, output: numpy array (n,)
    helpful_votes = subset_array_exponential(rng, helpful_votes, n, 
                                             scale = scale, sort = False)

    # generate review years, output: numpy array (n,)
    review_years = subset_array_exponential(rng, review_years, n, 
                                            scale = scale, sort = True)

    # generate review dates, add review years; output: pandas DataFrame with 2
    # columns
    review_dates_years = generate_dates(rng, review_years)

    # generate marketplace codes, output: numpy array (n,)
    marketplace = generate_marketplace_array(rng, n, scale = scale, 
                                             factor = marketplace_factor)
    
    # assemble the results into pandas DataFrame

    dat = review_dates_years
    dat["review_year"] = dat["review_year"].astype("int")
    dat["review_date"] = pd.to_datetime(dat["review_date"])
    dat["product_category"] = product_category
    dat["marketplace"] = marketplace
    dat["customer_id"] = cust_ins_ready[0, :]
    dat["insight"] = cust_ins_ready[1, :]
    dat["review_id"] = review_id
    dat["product_title"] = products[0, :]
    dat["product_id"] = products[1, :]
    dat["star_rating"] =  star_rating
    dat["star_rating"] = dat["star_rating"].astype("int")
    dat["helpful_votes"] = helpful_votes
    dat["helpful_votes"] = dat["helpful_votes"].astype("int")
    dat["total_votes"] = total_votes
    dat["total_votes"] = dat["total_votes"].astype("int")

    return dat


    
  





    