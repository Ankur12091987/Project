# Importing libraries
import pandas as pd
import gspread as gs
import snowflake
import pygsheets
from snowflake.snowpark import Session

# Setting up connection with the gsheet
gc = pygsheets.authorize(service_file='/home/ec2-user/Transferred_files/client_secret_json_files/client_secret-b.json')

sh = gc.open_by_key('16bksPoIE4vRfH37nI-VUBa_qZx2_BIh81aR7Ju7P6b8')
ws = sh.worksheet('title','FIRE')

# Loading the data from gsheet into dataframe
data = pd.DataFrame(ws.get_all_records())
print(data)

print("------------------CMO DISCOVERY STAGE STARTS---------------")

# CMO DISCOVERY
def cmo_score_q1(row):
    if row['What are your top 3 business problems/goals this FY?.1'] == 'Partial':
        return 2
    elif row['What are your top 3 business problems/goals this FY?.1'] == 'No':
        return 1
    elif row['What are your top 3 business problems/goals this FY?.1'] == 'Yes':
        return 3
    elif row['What are your top 3 business problems/goals this FY?.1'] == '':
        return 0


def cmo_score_q2(row):

    if row['Why are your top 3 business problems?.1'] == 'Partial':
        return 2
    elif row['Why are your top 3 business problems?.1'] == 'No':
        return 1
    elif row['Why are your top 3 business problems?.1'] == 'Yes':
        return 3
    elif row['Why are your top 3 business problems?.1'] == '':
        return 0


def cmo_score_q3(row):
    if row['What must success look like and how will you measure value?.1'] == 'Partial':
        return 2
    elif row['What must success look like and how will you measure value?.1'] == 'No':
        return 1
    elif row['What must success look like and how will you measure value?.1'] == 'Yes':
        return 3
    elif row['What must success look like and how will you measure value?.1'] == '':
        return 0


def cmo_score_q4(row):
    if row['Ideally when would leadership need to see some evidence of impact and value (leading success measures)?.1'] == 'Partial':
        return 2
    elif row['Ideally when would leadership need to see some evidence of impact and value (leading success measures)?.1'] == 'No':
        return 1
    elif row['Ideally when would leadership need to see some evidence of impact and value (leading success measures)?.1'] == 'Yes':
        return 3
    elif row['Ideally when would leadership need to see some evidence of impact and value (leading success measures)?.1'] == '':
        return 0


# creating the question level score as a separate column
data['cmo_score_q1'] = data.apply(lambda row: cmo_score_q1(row), axis=1)
data['cmo_score_q2'] = data.apply(lambda row: cmo_score_q2(row), axis=1)
data['cmo_score_q3'] = data.apply(lambda row: cmo_score_q3(row), axis=1)
data['cmo_score_q4'] = data.apply(lambda row: cmo_score_q4(row), axis=1)


print("----------CMO MARKET ADOPTION STAGE STARTS------------")

#data['How many solution adopter customers do you have?.1'] = data['How many solution adopter customers do you have?.1'].replace("", float("nan"))
data['How many solution adopter customers do you have?.1'] = data['How many solution adopter customers do you have?.1'].astype(str)


def cro_ma_score_q1(row):
    val = row['How many solution adopter customers do you have?.1']
    if val == 'Yes':
        return 3
    elif val == 'No':
        return 1
    elif val == 'Partial':
        return 2
    elif val == '':
        return 0
    else:
        return float("nan")

def cro_ma_score_q2(row):
    if row['Are your solution/product sales increasing YoY?.1'] == '15% AAGR':
        return 2
    elif row['Are your solution/product sales increasing YoY?.1'] == '<15 AAGR':
        return 1
    elif row['Are your solution/product sales increasing YoY?.1'] == '>15% AAGR':
        return 3
    elif row['Are your solution/product sales increasing YoY?.1'] == '':
        return 0


# creating the question level score as a separate column
data['cro_ma_score_q1'] = data.apply(lambda row: cro_ma_score_q1(row), axis=1)
data['cro_ma_score_q2'] = data.apply(lambda row: cro_ma_score_q2(row), axis=1)
# print(data)

print("----------------CRO DEMAND STAGE STARTS---------------")


def cro_demand_score_q1(row):
    if row['Do you apply a recency, frequency, monetary value analysis across all your customers to help define your best-fit ICPs? Do you repeat this process at least annually and/or each time you enter into a new customer segment?.1'] == 'Yes':
        return 3
    elif row['Do you apply a recency, frequency, monetary value analysis across all your customers to help define your best-fit ICPs? Do you repeat this process at least annually and/or each time you enter into a new customer segment?.1'] == 'Partial':
        return 2
    elif row['Do you apply a recency, frequency, monetary value analysis across all your customers to help define your best-fit ICPs? Do you repeat this process at least annually and/or each time you enter into a new customer segment?.1'] == 'No':
        return 1
    elif row['Do you apply a recency, frequency, monetary value analysis across all your customers to help define your best-fit ICPs? Do you repeat this process at least annually and/or each time you enter into a new customer segment?.1'] == '':
        return 0


def cro_demand_score_q2(row):
    if row['How large is your ICP TAM?.1'] == 'Yes':
        return 3
    elif row['How large is your ICP TAM?.1'] == 'Partial':
        return 2
    elif row['How large is your ICP TAM?.1'] == 'No':
        return 1
    elif row['How large is your ICP TAM?.1'] == '':
        return 0
    

def cro_demand_score_q3(row):
    if row['Do you intentionally identify and target cohorts of lookalike businesses where you have established a strong ICP fit to your solutions and have client references or case studies, so that you can expand by segment?.1'] == 'Yes':
        return 3
    elif row['Do you intentionally identify and target cohorts of lookalike businesses where you have established a strong ICP fit to your solutions and have client references or case studies, so that you can expand by segment?.1'] == 'Partial':
        return 2
    elif row['Do you intentionally identify and target cohorts of lookalike businesses where you have established a strong ICP fit to your solutions and have client references or case studies, so that you can expand by segment?.1'] == 'No':
        return 1
    elif row['Do you intentionally identify and target cohorts of lookalike businesses where you have established a strong ICP fit to your solutions and have client references or case studies, so that you can expand by segment?.1'] == '':
        return 0
    

def cro_demand_score_q4(row):
    if row['What is your AoV? If >$100K per unit, consider ABM as a potential strategy, if <$100K consider demand gen?.1'] == 'ABM':
        return 3
    elif row['What is your AoV? If >$100K per unit, consider ABM as a potential strategy, if <$100K consider demand gen?.1'] == 'Demand Gen':
        return 2
    elif row['What is your AoV? If >$100K per unit, consider ABM as a potential strategy, if <$100K consider demand gen?.1'] == 'Both':
        return 1
    elif row['What is your AoV? If >$100K per unit, consider ABM as a potential strategy, if <$100K consider demand gen?.1'] == '':
        return 0


def cro_demand_score_q5(row):
    if row['What are your demand unit waterfall metrics today?.1'] == 'Optimal throughout' or \
            row['What are your demand unit waterfall metrics today?.1'] == 'TOFU sub-optimal':
        return 3
    elif row['What are your demand unit waterfall metrics today?.1'] == 'MOFU sub-optimal' or \
            row['What are your demand unit waterfall metrics today?.1'] == 'BOFU sub-optimal':
        return 2
    elif row['What are your demand unit waterfall metrics today?.1'] == 'Marketing to Sales sub-optimal':
        return 1
    elif row['What are your demand unit waterfall metrics today?.1'] == '':
        return 0


def cro_demand_score_q6(row):
    if row['What is your CAC/LTV ratio?.1'] == 'Yes':
        return 3
    elif row['What is your CAC/LTV ratio?.1'] == 'On a par':
        return 2
    elif row['What is your CAC/LTV ratio?.1'] == 'No':
        return 1
    elif row['What is your CAC/LTV ratio?.1'] == '':
        return 0


def cro_demand_score_q7(row):
    if row[
        'Do you prioritize customer acquisitions over expansion or retention, if so, by how much and why?.1'] == 'Yes':
        return 3
    elif row[
        'Do you prioritize customer acquisitions over expansion or retention, if so, by how much and why?.1'] == 'Partial':
        return 2
    elif row[
        'Do you prioritize customer acquisitions over expansion or retention, if so, by how much and why?.1'] == 'No':
        return 1
    elif row[
        'Do you prioritize customer acquisitions over expansion or retention, if so, by how much and why?.1'] == '':
        return 0


def cro_demand_score_q8(row):
    if row['How do you measure outcomes and success/ROI across this entire journey?.1'] == 'Yes':
        return 3
    elif row['How do you measure outcomes and success/ROI across this entire journey?.1'] == 'Partial':
        return 2
    elif row['How do you measure outcomes and success/ROI across this entire journey?.1'] == 'No':
        return 1
    elif row['How do you measure outcomes and success/ROI across this entire journey?.1'] == '':
        return 0


def cro_demand_score_q9(row):
    if row['Do >70% of your sales reps hit >80% of their quarterly sales quota, every quarter?.1'] == 'Yes':
        return 3
    elif row['Do >70% of your sales reps hit >80% of their quarterly sales quota, every quarter?.1'] == 'Partial':
        return 2
    elif row['Do >70% of your sales reps hit >80% of their quarterly sales quota, every quarter?.1'] == 'No':
        return 1
    elif row['Do >70% of your sales reps hit >80% of their quarterly sales quota, every quarter?.1'] == '':
        return 0
    

def cro_demand_score_q10(row):
    if row['Does marketing support customer lifetime value marketing, by tracking all customers, and alerting your Customer Success and/or Account Management teams with any behaviour insights that may indicate either a churn risk, or upsell/cross-sell opportunities.1'] == 'Yes':
        return 3
    elif row['Does marketing support customer lifetime value marketing, by tracking all customers, and alerting your Customer Success and/or Account Management teams with any behaviour insights that may indicate either a churn risk, or upsell/cross-sell opportunities.1'] == 'Partial':
        return 2
    elif row['Does marketing support customer lifetime value marketing, by tracking all customers, and alerting your Customer Success and/or Account Management teams with any behaviour insights that may indicate either a churn risk, or upsell/cross-sell opportunities.1'] == 'No':
        return 1
    elif row['Does marketing support customer lifetime value marketing, by tracking all customers, and alerting your Customer Success and/or Account Management teams with any behaviour insights that may indicate either a churn risk, or upsell/cross-sell opportunities.1'] == '':
        return 0
    

def cro_demand_score_q11(row):
    if row['How aligned are your marketing & sales teams (very aligned, share goals/targets and collaborate on ' \
           'insights/activity/outcomes through to very siloed)?.1'] == 'Yes':
        return 3
    elif row['How aligned are your marketing & sales teams (very aligned, share goals/targets and collaborate on ' \
             'insights/activity/outcomes through to very siloed)?.1'] == 'Partial':
        return 2
    elif row['How aligned are your marketing & sales teams (very aligned, share goals/targets and collaborate on ' \
             'insights/activity/outcomes through to very siloed)?.1'] == 'No':
        return 1
    elif row['How aligned are your marketing & sales teams (very aligned, share goals/targets and collaborate on ' \
             'insights/activity/outcomes through to very siloed)?.1'] == '':
        return 0


# creating the question level score as a separate column
data['cro_demand_score_q1'] = data.apply(lambda row: cro_demand_score_q1(row), axis=1)
data['cro_demand_score_q2'] = data.apply(lambda row: cro_demand_score_q2(row), axis=1)
data['cro_demand_score_q3'] = data.apply(lambda row: cro_demand_score_q3(row), axis=1)
data['cro_demand_score_q4'] = data.apply(lambda row: cro_demand_score_q4(row), axis=1)
data['cro_demand_score_q5'] = data.apply(lambda row: cro_demand_score_q5(row), axis=1)
data['cro_demand_score_q6'] = data.apply(lambda row: cro_demand_score_q6(row), axis=1)
data['cro_demand_score_q7'] = data.apply(lambda row: cro_demand_score_q7(row), axis=1)
data['cro_demand_score_q8'] = data.apply(lambda row: cro_demand_score_q8(row), axis=1)
data['cro_demand_score_q9'] = data.apply(lambda row: cro_demand_score_q9(row), axis=1)
data['cro_demand_score_q10'] = data.apply(lambda row: cro_demand_score_q10(row), axis=1)
data['cro_demand_score_q11'] = data.apply(lambda row: cro_demand_score_q11(row), axis=1)

print("--------------MOPS TECH STACK-------------")


def mops_tech_score_q1(row):
    if row['Do you have a CDP?.1'] == 'Yes':
        return 3
    elif row['Do you have a CDP?.1'] == 'Partial':
        return 2
    elif row['Do you have a CDP?.1'] == 'No':
        return 1
    elif row['Do you have a CDP?.1'] == '':
        return 0


def mops_tech_score_q2(row):
    if row['Do you have an ABM platform?.1'] == 'Yes':
        return 3
    elif row['Do you have an ABM platform?.1'] == 'Partial':
        return 2
    elif row['Do you have an ABM platform?.1'] == 'No':
        return 1
    elif row['Do you have an ABM platform?.1'] == '':
        return 0


def mops_tech_score_q3(row):
    if row['Can you obtain persona-level contact data?.1'] == 'Yes':
        return 3
    elif row['Can you obtain persona-level contact data?.1'] == 'Partial':
        return 2
    elif row['Can you obtain persona-level contact data?.1'] == 'No':
        return 1
    elif row['Can you obtain persona-level contact data?.1'] == '':
        return 0


def mops_tech_score_q4(row):
    if row['Can you activate your data across display, social, email?.1'] == 'Yes':
        return 3
    elif row['Can you activate your data across display, social, email?.1'] == 'Partial':
        return 2
    elif row['Can you activate your data across display, social, email?.1'] == 'No':
        return 1
    elif row['Can you activate your data across display, social, email?.1'] == '':
        return 0


def mops_tech_score_q5(row):
    if row['Do you push MQLs into a SEP and/or CRM for next best action?.1'] == 'Yes':
        return 3
    elif row['Do you push MQLs into a SEP and/or CRM for next best action?.1'] == 'Partial':
        return 2
    elif row['Do you push MQLs into a SEP and/or CRM for next best action?.1'] == 'No':
        return 1
    elif row['Do you push MQLs into a SEP and/or CRM for next best action?.1'] == '':
        return 0


def mops_tech_score_q6(row):
    if row['Can you attribute and report on outcomes by campaign/channel/source?.1'] == 'Yes':
        return 3
    elif row['Can you attribute and report on outcomes by campaign/channel/source?.1'] == 'Partial':
        return 2
    elif row['Can you attribute and report on outcomes by campaign/channel/source?.1'] == 'No':
        return 1
    elif row['Can you attribute and report on outcomes by campaign/channel/source?.1'] == '':
        return 0

def mops_tech_score_q7(row):
    if row['How is marketing lead/opportunity data shared with sales?.1'] == 'Yes':
        return 3
    elif row['How is marketing lead/opportunity data shared with sales?.1'] == 'Partial':
        return 2
    elif row['How is marketing lead/opportunity data shared with sales?.1'] == 'No':
        return 1
    elif row['How is marketing lead/opportunity data shared with sales?.1'] == '':
        return 0


# MOPS TECH STACK
data['mops_tech_score_q1'] = data.apply(lambda row: mops_tech_score_q1(row), axis=1)
data['mops_tech_score_q2'] = data.apply(lambda row: mops_tech_score_q2(row), axis=1)
data['mops_tech_score_q3'] = data.apply(lambda row: mops_tech_score_q3(row), axis=1)
data['mops_tech_score_q4'] = data.apply(lambda row: mops_tech_score_q4(row), axis=1)
data['mops_tech_score_q5'] = data.apply(lambda row: mops_tech_score_q5(row), axis=1)
data['mops_tech_score_q6'] = data.apply(lambda row: mops_tech_score_q6(row), axis=1)
data['mops_tech_score_q7'] = data.apply(lambda row: mops_tech_score_q6(row), axis=1)

print("--------MOPS CONTENT FIR STAGE STARTS---------------")


def mops_content_score_q1(row):
    if row['Do you have content directly aimed at each target industry or ICP segment? Does this include specific ' \
           'value propositions, messaging and case studies for each segment?.1'] == 'Yes':
        return 3
    elif row['Do you have content directly aimed at each target industry or ICP segment? Does this include specific ' \
             'value propositions, messaging and case studies for each segment?.1'] == 'Partial':
        return 2
    elif row['Do you have content directly aimed at each target industry or ICP segment? Does this include specific ' \
             'value propositions, messaging and case studies for each segment?.1'] == 'No':
        return 1
    elif row['Do you have content directly aimed at each target industry or ICP segment? Does this include specific ' \
             'value propositions, messaging and case studies for each segment?.1'] == '':
        return 0


def mops_content_score_q2(row):
    if row['Do you have content aimed specifically at a researcher, user, buyer and decision maker for each use case ' \
           'or solution?.1'] == 'Yes':
        return 3
    elif row['Do you have content aimed specifically at a researcher, user, buyer and decision maker for each use ' \
             'case or solution?.1'] == 'Partial':
        return 2
    elif row['Do you have content aimed specifically at a researcher, user, buyer and decision maker for each use ' \
             'case or solution?.1'] == 'No':
        return 1
    elif row['Do you have content aimed specifically at a researcher, user, buyer and decision maker for each use ' \
             'case or solution?.1'] == '':
        return 0


# MOPS TECH STACK
data['mops_content_score_q1'] = data.apply(lambda row: mops_content_score_q1(row), axis=1)
data['mops_content_score_q2'] = data.apply(lambda row: mops_content_score_q2(row), axis=1)

print("---------------MOPS CHALLENGER CONNEX STAGE STARTS---------------")


def mops_challenger_score_q1(row):
    if row['Can you segment your total reachable audience (TRA) into Brand vs Demand segments + account-level using ' \
           'intent/engagement insights and therefore personalize your outreach messages? Can you personalize your ' \
           'inbound experience?.1'] == 'Yes':
        return 3
    elif row['Can you segment your total reachable audience (TRA) into Brand vs Demand segments + account-level using ' \
             'intent/engagement insights and therefore personalize your outreach messages? Can you personalize your ' \
             'inbound experience?.1'] == 'Partial':
        return 2
    elif row['Can you segment your total reachable audience (TRA) into Brand vs Demand segments + account-level using ' \
             'intent/engagement insights and therefore personalize your outreach messages? Can you personalize your ' \
             'inbound experience?.1'] == 'No':
        return 1
    elif row['Can you segment your total reachable audience (TRA) into Brand vs Demand segments + account-level using ' \
             'intent/engagement insights and therefore personalize your outreach messages? Can you personalize your ' \
             'inbound experience?.1'] == '':
        return 0


def mops_challenger_score_q2(row):
    if row['Can you identify all anonymous visitors and map them to an account?.1'] == 'Yes':
        return 3
    elif row['Can you identify all anonymous visitors and map them to an account?.1'] == 'Partial':
        return 2
    elif row['Can you identify all anonymous visitors and map them to an account?.1'] == 'No':
        return 1
    elif row['Can you identify all anonymous visitors and map them to an account?.1'] == '':
        return 0


# MOPS TECH STACK
data['mops_challenger_score_q1'] = data.apply(lambda row: mops_challenger_score_q1(row), axis=1)
data['mops_challenger_score_q2'] = data.apply(lambda row: mops_challenger_score_q2(row), axis=1)


print("---------------VALUE PROPOSITION STARTS---------------")

data["Core Feature 1"] = data["Core Feature 1"].replace("", float("nan"))
data["Core Feature 2"] = data["Core Feature 2"].replace("", float("nan"))
data["Ability to Execute"] = data["Ability to Execute"].replace("", float("nan"))
data["Future Roadmap"] = data["Future Roadmap"].replace("", float("nan"))

def value_proposition_q1(row):
    val = row['Core Feature 1']
    if val == 3:
        return 3
    elif val == 1:
        return 1
    elif val == 2:
        return 2
    elif val == '':
        return 0
    else:
        return float("nan")

def value_proposition_q2(row):
    val = row['Core Feature 2']
    if val == 2:
        return 2
    elif val == 1:
        return 1
    elif val == 3:
        return 3
    elif val == '':
        return 0
    else:
        return float("nan")

def value_proposition_q3(row):
    val = row['Ability to Execute']
    if val == 2:
        return 2
    elif val == 1:
        return 1
    elif val == 3:
        return 3
    elif val == '':
        return 0
    else:
        return float("nan")

def value_proposition_q4(row):
    val = row['Future Roadmap']
    if val == 2:
        return 2
    elif val == 1:
        return 1
    elif val == 3:
        return 3
    elif val == '':
        return 0
    else:
        return float("nan")

# VALUE PROPOSITION
data['value_proposition_q1'] = data.apply(lambda row: value_proposition_q1(row), axis=1)
data['value_proposition_q2'] = data.apply(lambda row: value_proposition_q2(row), axis=1)
data['value_proposition_q3'] = data.apply(lambda row: value_proposition_q3(row), axis=1)
data['value_proposition_q4'] = data.apply(lambda row: value_proposition_q4(row), axis=1)


print("---------------BRAND---------------")
data["Unprompted"] = data["Unprompted"].replace("", float("nan"))
data["Prompted"] = data["Prompted"].replace("", float("nan"))
data["Trust"] = data["Trust"].replace("", float("nan"))
data["Mastery"] = data["Mastery"].replace("", float("nan"))
data["Compassion"] = data["Compassion"].replace("", float("nan"))
data["Transformative"] = data["Transformative"].replace("", float("nan"))
data["Brand Engagement"] = data["Brand Engagement"].replace("", float("nan"))
data["Brand Consideration"] = data["Brand Consideration"].replace("", float("nan"))
data["Brand Advocacy"] = data["Brand Advocacy"].replace("", float("nan"))



def brand_q1(row):
    val = row['Unprompted']
    if val == 2:
        return 2
    elif val == 1:
        return 1
    elif val == 3:
        return 3
    elif val == '':
        return 0
    else:
        return float("nan")
    
def brand_q2(row):
    val = row['Prompted']
    if val == 2:
        return 2
    elif val == 1:
        return 1
    elif val == 3:
        return 3
    elif val == '':
        return 0
    else:
        return float("nan")


def brand_q3(row):
    val = row['Trust']
    if val == 2:
        return 2
    elif val == 1:
        return 1
    elif val == 3:
        return 3
    elif val == '':
        return 0
    else:
        return float("nan")


def brand_q4(row):
    val = row['Mastery']
    if val == 2:
        return 2
    elif val == 1:
        return 1
    elif val == 3:
        return 3
    elif val == '':
        return 0
    else:
        return float("nan")
    
def brand_q5(row):
    val = row['Compassion']
    if val == 2:
        return 2
    elif val == 1:
        return 1
    elif val == 3:
        return 3
    elif val == '':
        return 0
    else:
        return float("nan")

    
def brand_q6(row):
    val = row['Transformative']
    if val == 2:
        return 2
    elif val == 1:
        return 1
    elif val == 3:
        return 3
    elif val == '':
        return 0
    else:
        return float("nan")


def brand_q7(row):
    val = row['Brand Engagement']
    if val == 2:
        return 2
    elif val == 1:
        return 1
    elif val == 3:
        return 3
    elif val == '':
        return 0
    else:
        return float("nan")
    
def brand_q8(row):
    val = row['Brand Consideration']
    if val == 2:
        return 2
    elif val == 1:
        return 1
    elif val == 3:
        return 3
    elif val == '':
        return 0
    else:
        return float("nan")


def brand_q9(row):
    val = row['Brand Advocacy']
    if val == 2:
        return 2
    elif val == 1:
        return 1
    elif val == 3:
        return 3
    elif val == '':
        return 0
    else:
        return float("nan")



# BRAND
data['brand_q1'] = data.apply(lambda row: brand_q1(row), axis=1)
data['brand_q2'] = data.apply(lambda row: brand_q2(row), axis=1)
data['brand_q3'] = data.apply(lambda row: brand_q3(row), axis=1)
data['brand_q4'] = data.apply(lambda row: brand_q4(row), axis=1)
data['brand_q5'] = data.apply(lambda row: brand_q5(row), axis=1)
data['brand_q6'] = data.apply(lambda row: brand_q6(row), axis=1)
data['brand_q7'] = data.apply(lambda row: brand_q7(row), axis=1)
data['brand_q8'] = data.apply(lambda row: brand_q8(row), axis=1)
data['brand_q9'] = data.apply(lambda row: brand_q9(row), axis=1)


print("--------MOPS AUDIENCE FIR STAGE STARTS---------------")

def mops_audience_score_q1(row):
    if row['Do you have and use account-based intent and tech/install + contract renewal insights to help with account prioritisation?.1'] == 'Yes':
        return 3
    elif row['Do you have and use account-based intent and tech/install + contract renewal insights to help with account prioritisation?.1'] == 'Partial':
        return 2
    elif row['Do you have and use account-based intent and tech/install + contract renewal insights to help with account prioritisation?.1'] == 'No':
        return 1
    elif row['Do you have and use account-based intent and tech/install + contract renewal insights to help with account prioritisation?.1'] == '':
        return 0
    

def mops_audience_score_q2(row):
    if row['Do you split your TAM into brand vs demand segments and adjust your outreach messaging and campaign approach accordingly?.1'] == 'Yes':
        return 3
    elif row['Do you split your TAM into brand vs demand segments and adjust your outreach messaging and campaign approach accordingly?.1'] == 'Partial':
        return 2
    elif row['Do you split your TAM into brand vs demand segments and adjust your outreach messaging and campaign approach accordingly?.1'] == 'No':
        return 1
    elif row['Do you split your TAM into brand vs demand segments and adjust your outreach messaging and campaign approach accordingly?.1'] == '':
        return 0
    

def mops_audience_score_q3(row):
    if row['Do you drive audiences to personalized landing pages and content, based upon account insights?.1'] == 'Yes':
        return 3
    elif row['Do you drive audiences to personalized landing pages and content, based upon account insights?.1'] == 'Partial':
        return 2
    elif row['Do you drive audiences to personalized landing pages and content, based upon account insights?.1'] == 'No':
        return 1
    elif row['Do you drive audiences to personalized landing pages and content, based upon account insights?.1'] == '':
        return 0  
    

def mops_audience_score_q4(row):
    if row['Can you track engagement at account level and use this to push accounts through the marketing nurture funnel and through to sales activation?.1'] == 'Yes':
        return 3
    elif row['Can you track engagement at account level and use this to push accounts through the marketing nurture funnel and through to sales activation?.1'] == 'Partial':
        return 2
    elif row['Can you track engagement at account level and use this to push accounts through the marketing nurture funnel and through to sales activation?.1'] == 'No':
        return 1
    elif row['Can you track engagement at account level and use this to push accounts through the marketing nurture funnel and through to sales activation?.1'] == '':
        return 0 
    

def mops_audience_score_q5(row):
    if row['Do you have a sales activation (BDR team) who use marketing insights to personalize their outreach?.1'] == 'Yes':
        return 3
    elif row['Do you have a sales activation (BDR team) who use marketing insights to personalize their outreach?.1'] == 'Partial':
        return 2
    elif row['Do you have a sales activation (BDR team) who use marketing insights to personalize their outreach?.1'] == 'No':
        return 1
    elif row['Do you have a sales activation (BDR team) who use marketing insights to personalize their outreach?.1'] == '':
        return 0 
    

def mops_audience_score_q6(row):
    if row['Do your BDR teams qualify interest, need and purchase intent prior to handing over to the sales team?.1'] == 'Yes':
        return 3
    elif row['Do your BDR teams qualify interest, need and purchase intent prior to handing over to the sales team?.1'] == 'Partial':
        return 2
    elif row['Do your BDR teams qualify interest, need and purchase intent prior to handing over to the sales team?.1'] == 'No':
        return 1
    elif row['Do your BDR teams qualify interest, need and purchase intent prior to handing over to the sales team?.1'] == '':
        return 0 


def mops_audience_score_q7(row):
    if row['Do you have a fully optimized brand + demand creation engine?.1'] == 'Yes':
        return 3
    elif row['Do you have a fully optimized brand + demand creation engine?.1'] == 'Partial':
        return 2
    elif row['Do you have a fully optimized brand + demand creation engine?.1'] == 'No':
        return 1
    elif row['Do you have a fully optimized brand + demand creation engine?.1'] == '':
        return 0 
    

def mops_audience_score_q8(row):
    if row['Do you operate in a new market category, or a market niche, that is significant enough to help fuel demand and be difficult for others to replicate quickly?you have a fully optimized brand + demand creation engine?.1'] == 'Yes':
        return 3
    elif row['Do you operate in a new market category, or a market niche, that is significant enough to help fuel demand and be difficult for others to replicate quickly?you have a fully optimized brand + demand creation engine?.1'] == 'Partial':
        return 2
    elif row['Do you operate in a new market category, or a market niche, that is significant enough to help fuel demand and be difficult for others to replicate quickly?you have a fully optimized brand + demand creation engine?.1'] == 'No':
        return 1
    elif row['Do you operate in a new market category, or a market niche, that is significant enough to help fuel demand and be difficult for others to replicate quickly?you have a fully optimized brand + demand creation engine?.1'] == '':
        return 0 
    

def mops_audience_score_q9(row):
    if row['Do you have a marketing and sales tech stack that enables you to consistently deliver 1:1 experiences across the entire customer buying journey (acquisition/retention/expansion) across a 1:Many market audience?.1'] == 'Yes':
        return 3
    elif row['Do you have a marketing and sales tech stack that enables you to consistently deliver 1:1 experiences across the entire customer buying journey (acquisition/retention/expansion) across a 1:Many market audience?.1'] == 'Partial':
        return 2
    elif row['Do you have a marketing and sales tech stack that enables you to consistently deliver 1:1 experiences across the entire customer buying journey (acquisition/retention/expansion) across a 1:Many market audience?.1'] == 'No':
        return 1
    elif row['Do you have a marketing and sales tech stack that enables you to consistently deliver 1:1 experiences across the entire customer buying journey (acquisition/retention/expansion) across a 1:Many market audience?.1'] == '':
        return 0     
    

def mops_audience_score_q10(row):
    if row['Can you deliver effective ABM to your enterprise accounts?.1'] == 'Yes':
        return 3
    elif row['Can you deliver effective ABM to your enterprise accounts?.1'] == 'Partial':
        return 2
    elif row['Can you deliver effective ABM to your enterprise accounts?.1'] == 'No':
        return 1
    elif row['Can you deliver effective ABM to your enterprise accounts?.1'] == '':
        return 0   
    


# MOPS TECH STACK
data['mops_audience_score_q1'] = data.apply(lambda row: mops_audience_score_q1(row), axis=1)
data['mops_audience_score_q2'] = data.apply(lambda row: mops_audience_score_q2(row), axis=1)
data['mops_audience_score_q3'] = data.apply(lambda row: mops_audience_score_q3(row), axis=1)
data['mops_audience_score_q4'] = data.apply(lambda row: mops_audience_score_q4(row), axis=1)
data['mops_audience_score_q5'] = data.apply(lambda row: mops_audience_score_q5(row), axis=1)
data['mops_audience_score_q6'] = data.apply(lambda row: mops_audience_score_q6(row), axis=1)
data['mops_audience_score_q7'] = data.apply(lambda row: mops_audience_score_q7(row), axis=1)
data['mops_audience_score_q8'] = data.apply(lambda row: mops_audience_score_q8(row), axis=1)
data['mops_audience_score_q9'] = data.apply(lambda row: mops_audience_score_q9(row), axis=1)
data['mops_audience_score_q10'] = data.apply(lambda row: mops_audience_score_q10(row), axis=1)

# Creating sectional scores
data['total_cmo_score'] = data['cmo_score_q1'] + data['cmo_score_q2'] + data['cmo_score_q3'] + data['cmo_score_q4']
data['total_cro_demand_score'] = data['cro_demand_score_q1'] + data['cro_demand_score_q2'] + data[
    'cro_demand_score_q3'] + data['cro_demand_score_q4'] + data['cro_demand_score_q5'] + data['cro_demand_score_q6'] + data['cro_demand_score_q7'] + data['cro_demand_score_q8'] + data['cro_demand_score_q9'] + data['cro_demand_score_q10'] + data['cro_demand_score_q11']
data['total_cro_ma_score'] = data['cro_ma_score_q1'] + data['cro_ma_score_q2']
data['total_mops_tech_score'] = data['mops_tech_score_q1'] + data['mops_tech_score_q2'] + data['mops_tech_score_q3'] + data['mops_tech_score_q4'] + data['mops_tech_score_q5'] + data['mops_tech_score_q6'] + data['mops_tech_score_q7']
data['total_mops_content_score'] = data['mops_content_score_q1'] + data['mops_content_score_q2']
data['total_mops_challenger_score'] = data['mops_challenger_score_q1'] + data['mops_challenger_score_q2']
data['total_proposition_score'] = data['value_proposition_q1'].fillna(0) + data['value_proposition_q2'].fillna(0) + data['value_proposition_q3'].fillna(0) + data['value_proposition_q4'].fillna(0)
data['total_brand_score'] = data['brand_q1'].fillna(0) + data['brand_q2'].fillna(0) + data['brand_q3'].fillna(0) + data['brand_q4'].fillna(0) + data['brand_q5'].fillna(0) + data['brand_q6'].fillna(0) + data['brand_q7'].fillna(0) + data['brand_q8'].fillna(0) + data['brand_q9'].fillna(0)
data['total_mops_audience_score'] = data['mops_audience_score_q1'] + data['mops_audience_score_q2'] + data['mops_audience_score_q3'] + data['mops_audience_score_q4'] + data['mops_audience_score_q5'] + data['mops_audience_score_q6'] + data['mops_audience_score_q7'] + data['mops_audience_score_q8'] + data['mops_audience_score_q9'] + data['mops_audience_score_q10']

print("----------SNOWFLAKE CONNECTION STARTED--------------")
# Connect to Snowflake
connection_parameters = {
    "account": "nua76068.us-east-1",
    "user": "AniketK",
    "password": "Follower@123",
    "role": "ACCOUNTADMIN",  # optional
    "warehouse": "COMPUTE_WH",  # optional
    "database": "MY_WORKSPACE",  # optional
    "schema": "ANIKET",  # optional
}

new_session = Session.builder.configs(connection_parameters).create()

my_list = ['Please enter the name of the Client',
           'total_cmo_score',
           'total_cro_demand_score',
           'total_cro_ma_score',
           'total_mops_tech_score',
           'total_mops_content_score',
           'total_mops_challenger_score',
           'total_proposition_score',
           'total_brand_score',
           'total_mops_audience_score']

data_new = data[my_list]


data_new.rename(columns={'Please enter the name of the Client': 'Client'}, inplace=True)
# df = pd.read_csv('response.csv')
data  = data.applymap(lambda x: pd.to_numeric(x, errors='ignore') if str(x).replace('.', '', 1).isdigit() else x)
data = data.astype(float, errors='ignore')

data_new  = data_new.applymap(lambda x: pd.to_numeric(x, errors='ignore') if str(x).replace('.', '', 1).isdigit() else x)
data_new = data_new.astype(float, errors='ignore')

df1 = new_session.create_dataframe(data_new)
df2 = new_session.create_dataframe(data)
print(df1)
print(df2)
print('HELLO')
df1.write.mode("overwrite").save_as_table("FIRE_RESPONSE")
df2.write.mode("overwrite").save_as_table("FIRE_RESPONSE_RAW")

print("Data Load Successful")
