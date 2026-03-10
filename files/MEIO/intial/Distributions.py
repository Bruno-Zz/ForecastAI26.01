# -*- coding: utf-8 -*-
"""
Created on Fri Aug 11 17:21:56 2023

@author: BrunoZindy
"""

from scipy.stats import poisson , norm
import logging
import math
from scipy.special import ndtr
import numpy as np

maxEoqFreq=10
maxTestEBO=3

class Distribution:
    
    @staticmethod
    def poisson_distribution_FR2ROP (self, FR, eoq, mean, avgsize):
        rop=float(0);
        
        rop = poisson.ppf(FR, mean)*avgsize
        rop -= int(eoq)/2;
    
        return rop;
            
    @staticmethod
    def poisson_distribution_ROP2FR (self, rop, eoq, forecastlt, avgsize):
        total_fill_rate=float(0);
        loop_counter=0
        step= int(max(1, avgsize, math.ceil((eoq)/maxEoqFreq)))
        tested_eoq=0
        while tested_eoq <eoq+step:
            maxEoq=min(eoq, tested_eoq)
            total_fill_rate += poisson.cdf((rop+maxEoq)/avgsize, forecastlt, 0 )
            tested_eoq+=step
            loop_counter+=1
            
            logging.warning("???poisson_distribution - rop: " + str(rop) + " - tested_eoq: " + str(tested_eoq)  + " - avgsize: " + str(avgsize)  + " - forecastlt: " + str(forecastlt)  )

        return_fill_rate= total_fill_rate /loop_counter
        logging.warning("poisson_distribution - rop: " + str(rop) + " - eoq: " + str(eoq)  + " - loop_counter: " + str(loop_counter)  + " - avgsize: " + str(avgsize) + " total_fill_rate: " + str(total_fill_rate)+ " - return_fill_rate: " + str(return_fill_rate)  + " - forecastlt: " + str(forecastlt)  + " - tested_eoq: " + str(tested_eoq) )

        return return_fill_rate;
    
    @staticmethod
    def poisson_distribution_ROP2AVAIL (self, rop, eoq, forecastlt, avgsize):
        step= int(max(1, avgsize, math.ceil((eoq)/maxEoqFreq)))
        testedDemand=round(rop,0)
        totalEboDemand=0
        
        while (testedDemand<forecastlt*maxTestEBO):
            loop_eoq_counter=0
            eboEoq=0
            tested_eoq=0

            while tested_eoq <eoq+step:

                maxEoq=min(eoq, tested_eoq)
                eboEoq+=max(testedDemand-(rop+maxEoq)/avgsize,0)*poisson.pmf(testedDemand, forecastlt, loc=0 )
                tested_eoq+=step
                loop_eoq_counter+=1
                logging.warning("poisson_distribution_ROP2AVAIL - testedDemand: " + str(testedDemand) + " - maxEoq: " + str(maxEoq)  + " - eboEoq: " + str(eboEoq)  + " - avgsize: " + str(avgsize) + " rop: " + str(rop)+ " - step: " + str(step)  + " - forecastlt: " + str(forecastlt)  + " - loop_eoq_counter: " + str(loop_eoq_counter) )

            eboDemand=eboEoq/loop_eoq_counter
            
            totalEboDemand+=eboDemand
            testedDemand+=1

        logging.warning("poisson_distribution_ROP2AVAIL - testedDemand: " + str(testedDemand) + " - totalEboDemand: " + str(totalEboDemand)  + " - eboEoq: " + str(eboEoq)  + " - avgsize: " + str(avgsize) + " rop: " + str(rop)+ " - step: " + str(step)  + " - forecastlt: " + str(forecastlt)  + " - maxTestEBO: " + str(maxTestEBO) )

        return (1-totalEboDemand)
    
   
    
    @staticmethod
    def normal_distribution_FR2ROP  (self, FR, lead_time, forecastlt, avgsize, lead_time_stdev, dmd_stddev):
        rop=norm.ppf(FR, loc=forecastlt, scale=lead_time_stdev)*avgsize
        return (rop)

    def golden_formula_ROP2FR  (self, ROP, eoq, lead_time, forecastlt, avgsize, lead_time_stdev, dmd_stddev, mad):
        
        if np.isnan(mad) and dmd_stddev==0:
            return Distribution.normal_distribution_ROP2FR  (self, ROP, eoq, lead_time, forecastlt, avgsize, lead_time_stdev, dmd_stddev=forecastlt)
        
        total_fill_rate=float(0);
        
        loop_counter=0
        step= int(max(1, avgsize, math.ceil((eoq)/maxEoqFreq)))
        tested_eoq=0
        leadTimeMonth=lead_time/30
        #dailyRate=forecastlt/lead_time
        dailyRate=forecastlt*avgsize/lead_time
        #leadTimeDeviationPortion=(dailyRate * lead_time_stdev)**2 
        leadTimeDeviationPortion=((dailyRate) * (0 if np.isnan(lead_time_stdev) else lead_time_stdev) )**2
        
        logging.warning("1- golden_formula_ROP2FR - tested_eoq: " + str(tested_eoq) + " - eoq: " + str(eoq)  + " - avgsize: " + str(avgsize)  + " - forecastlt: " + str(forecastlt)  + " - step: " + str(step)   + " - total_fill_rate: " + str(total_fill_rate)  + " - loop_counter: " + str(loop_counter)  + " - mad: " + str(mad) + " - lead_time_stdev: " + str(lead_time_stdev) + " - added portion not 0 stddev lt: " + str(30/lead_time + ((forecastlt*30/lead_time) * lead_time_stdev)**2)  + " - leadTimeDeviationPortion: " + str(leadTimeDeviationPortion)  + " - dailyRate: " + str(dailyRate)  + " - leadTimeMonth: " + str(leadTimeMonth) )
        
        #leadTimeDeviationPortion=0
        
        while tested_eoq <(eoq+step):

            maxEoq=min(eoq, tested_eoq)
            
            fill_rate=0
            #safetyStock=ROP+tested_eoq-(forecastlt/avgsize)
            safetyStock=ROP+tested_eoq-(forecastlt*avgsize)

            fill_rate=0
            if mad is None or np.isnan(mad) :
                demandVariation= dmd_stddev
            else :
                #demandVariation= 1.25* mad* forecastlt * math.sqrt(1/leadTimeMonth)
                #mad should be computed as a line forecast accuracy
                demandVariation= 1.25* mad* forecastlt * avgsize * math.sqrt(1/leadTimeMonth)

                #fill_rate=ndtr(safetyStock / math.sqrt((1.25* mad* forecastlt)**2 * (1/leadTimeMonth)*lead_time + (forecastPerMonthInDays * lead_time_stdev)**2 )  )
                #fill_rate=ndtr(safetyStock / math.sqrt((1.25* mad* forecastlt)**2 * (1/leadTimeMonth)*lead_time + (forecastPerMonthInDays * lead_time_stdev)**2 )  )
                
            demandVariationPortion= math.sqrt(demandVariation**2  *lead_time)
            racine=math.sqrt(demandVariationPortion + leadTimeDeviationPortion)
            # Z=safetyStock /  racine
            fill_rate=ndtr(safetyStock /  racine )

            total_fill_rate += fill_rate
            tested_eoq+=step
            loop_counter+=1
            logging.warning("2- golden_formula_ROP2FR - tested_eoq: " + str(tested_eoq) + " - eoq: " + str(eoq)  + " - avgsize: " + str(avgsize)  + " - forecastlt: " + str(forecastlt)  + " - step: " + str(step)   + " - dmd_stddev: " + str(dmd_stddev)  + " - loop_counter: " + str(loop_counter)    + " - maxEoq: " + str(maxEoq)   + " - total_fill_rate: " + str(total_fill_rate)   + " - mad: " + str(mad)   + " - fill_rate: " + str(fill_rate)   + " - racine: " + str(racine)   + " - safetyStock: " + str(safetyStock)   + " - demandVariationPortion: " + str(demandVariationPortion)    + " - demandVariation: " + str(demandVariation)    + " - leadTimeDeviationPortion: " + str(leadTimeDeviationPortion)  )


        returned_fill_rate = total_fill_rate/loop_counter
        
        logging.warning("3- golden_formula_ROP2FR (returning)- tested_eoq: " + str(tested_eoq) + " - ROP: " + str(ROP)  + " - avgsize: " + str(avgsize)  + " - forecastlt: " + str(forecastlt)  + " - maxEoq: " + str(maxEoq)   + " - total_fill_rate: " + str(returned_fill_rate)  + " - loop_counter: " + str(loop_counter)    + " - mad: " + str(mad) )

        return returned_fill_rate;

    
    def normal_distribution_ROP2FR  (self, ROP, eoq, lead_time, forecastlt, avgsize, lead_time_stdev, dmd_stddev):
        total_fill_rate=float(0);
        loop_counter=0
        step= int(max(1, avgsize, math.ceil((eoq)/maxEoqFreq)))
        tested_eoq=0
        #logging.warning("1- normal_distribution_ROP2FR - tested_eoq: " + str(tested_eoq) + " - eoq: " + str(eoq)  + " - avgsize: " + str(avgsize)  + " - forecastlt: " + str(forecastlt)  + " - step: " + str(step)   + " - total_fill_rate: " + str(total_fill_rate)  + " - loop_counter: " + str(loop_counter)   )

        while tested_eoq <eoq+step:
            maxEoq=min(eoq, tested_eoq)
            total_fill_rate += norm.cdf((ROP+maxEoq)/avgsize, forecastlt, dmd_stddev );
            tested_eoq+=step
            loop_counter+=1
            #logging.warning("2- normal_distribution_ROP2FR - tested_eoq: " + str(tested_eoq) + " - eoq: " + str(eoq)  + " - avgsize: " + str(avgsize)  + " - forecastlt: " + str(forecastlt)  + " - step: " + str(step)   + " - dmd_stddev: " + str(dmd_stddev)  + " - loop_counter: " + str(loop_counter)    + " - maxEoq: " + str(maxEoq)   + " - total_fill_rate: " + str(total_fill_rate)   )


        total_fill_rate /= loop_counter
        
        #logging.warning("3- normal_distribution_ROP2FR - tested_eoq: " + str(tested_eoq) + " - ROP: " + str(ROP)  + " - avgsize: " + str(avgsize)  + " - forecastlt: " + str(forecastlt)  + " - maxEoq: " + str(maxEoq)   + " - total_fill_rate: " + str(total_fill_rate)  + " - loop_counter: " + str(loop_counter)   )

        return total_fill_rate;

