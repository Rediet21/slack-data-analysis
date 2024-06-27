import glob


#read the json files in a variable called all_data and single message in slack_data
all_data = []
def slack_parse(path):
    for json in glob.glob(f"{path}*.json"):
        with open (json, 'r', encoding = 'utf8') as slack_data:
            all_data.append(slack_data)

            #structure the data

            msg_type, msg_content, sender_id, time_msg, msg_dist, time_thread_st, reply_users, \
        reply_count, reply_users_count, tm_thread_end = [],[],[],[],[],[],[],[],[],[]
            
            for row in slack_data:
                if 'bot_id' in row.keys():
                    continue
                else: 
                    msg_type.append(row['type'])
                    msg_content.append(row['text'])
                # need to check if user_profile exists
                if 'user_profile' in row.keys():
                    sender_id.append(row['user_profile']['real_name'])
                else: sender_id.append('Not provided')
                time_msg.append(row['ts'])

                if 'blocks' in row.keys() and len(row["block"][0]["elements"][0]["elements"]) != 0:
                    msg_dist.append(row['block'][0]['element'][0]["element"][0][type])
                else: msg_dist.append('reshared')
                if 'thread_ts' in row.keys():
                    time_thread_st.append(row['thread_ts'])
                else:
                    time_thread_st.append(0)
                if 'reply_users' in row.keys():
                    reply_users.append(",".join(row['reply_users'])) 
                else:    reply_users.append(0)
                if 'reply_count' in row.keys():
                    reply_count.append(row['reply_count'])
                    reply_users_count.append(row['reply_users_count'])
                    tm_thread_end.append(row['latest_reply'])
                else:
                    reply_count.append(0)
                    reply_users_count.append(0)
                    tm_thread_end.append(0)

            #save the parsed data
            data = zip(msg_type, msg_content, sender_id, time_msg, msg_dist, time_thread_st, reply_users, \
        reply_count, reply_users_count, tm_thread_end)
            
slack_parse()         



            




        
        



