import glob
import pandas as pd
import os


#read the json files in a variable called all_data and single message in slack_data
all_data = []
def slack_parse(path):
    for json in glob.glob(f"{path}*.json"):
        with open (json, 'r', encoding = 'utf8') as slack_data:
            all_data.append(slack_data)

        df_List = []    #structure the data
        for slack_data in all_data:

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

                if 'blocks'in row.keys() and len(row["block"][0]["elements"][0]["elements"]) != 0:
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

            #save the parsed data to a zip file
            #sees the structure of the zipped data
            data = zip(msg_type, msg_content, sender_id, time_msg, msg_dist, time_thread_st, reply_users, \
        reply_count, reply_users_count, tm_thread_end)
            
            columns = ['msg_type', 'msg_content', 'sender_id', 'time_msg', 'msg_dist', 'time_thread_st', 'reply_users',
                        'reply_count','reply_count', 'reply_users_count', 'tm_thread_end']
            #change the data to a dataframe
            df = pd.Dataframe(data=data, columns=columns)
            df = df[df['sender_id'] != 'Not provided']
            df_List.append(df)

        dfall = pd.concatenate(df_List, ignore_index=True)
        dfall['channel'] = path.split('/')[-1].split('.')[0]
        dfall = dfall.reset_index(drop=True)
    
    
    return dfall
            
# Parsing through the reactions   (get Reactions)
def slach_reaction(path, channel):
    combined = []
    for json in glob.glob (f"{path}*json"):
        with open (json, 'r', encoding = "utf8") as slack_data:
            combined.append(slack_data)

        reaction_name, reaction_count, reaction_users, msg, user_id = [], [], [], [], []

    
        for k in combined:
            slack_data = json.load(open(k.name,'r', encoding = "utf-8"))

            for i in enumerate(slack_data): 
                if 'reaction' in i.keys():
                    for j in range(len(i['reactions'])):
                        reaction_name.append(i['reaction'][j]['names'])
                        reaction_users.append(i['reactions'][j]['count'])
                        reaction_users.append(','.join(i['reaction'][j]['users']))

        df_reaction = zip(reaction_name, reaction_count, reaction_users,msg, user_id)
        reaction_column = ['reaction_name', 'reaction_count', 'reaction_users_count', 'message', 'user_id']
        df_reaction = pd.DataFrame(data = df_reaction, columns = reaction_column)
        df_reaction['channel'] = channel
        return df_reaction
    
    def get_community_participation(path):

        """ specify path to get json files"""
        combined = []
        comm_dict = {}
        for json_file in glob.glob(f"{path}*.json"):
            with open(json_file, 'r') as slack_data:
                combined.append(slack_data)
        # print(f"Total json files is {len(combined)}")
        for i in combined:
            a = json.load(open(i.name, 'r', encoding='utf-8'))

            for msg in a:
                if 'replies' in msg.keys():
                    for i in msg['replies']:
                        comm_dict[i['user']] = comm_dict.get(i['user'], 0)+1
        return comm_dict
    

    class SlackDataLoader:
        '''
      

        When you open slack exported ZIP file, each channel or direct message 
        will have its own folder. Each folder will contain messages from the 
        conversation, organised by date in separate JSON files.

        You'll see reference files for different kinds of conversations: 
        users.json files for all types of users that exist in the slack workspace
        channels.json files for public channels, 
    
    These files contain metadata about the conversations, including their names and IDs.

    For secruity reason, we have annonymized names - the names you will see are generated using faker library.
    
    '''
        
    def __init__(self, path):
        '''
        path: path to the slack exported data folder
        '''
        self.path = path
        self.channels = self.get_channels()
        self.users = self.get_ussers()
    

    def get_users(self):
        '''
        write a function to get all the users from the json file
        '''
        with open(os.path.join(self.path, 'users.json'), 'r') as f:
            users = json.load(f)

        return users
    
    def get_channels(self):
        '''
        write a function to get all the channels from the json file
        '''
        with open(os.path.join(self.path, 'channels.json'), 'r') as f:
            channels = json.load(f)

        return channels

    def get_channel_messages(self, channel_name):
        '''
        write a function to get all the messages from a channel
        
        '''
        with open(os.path.join(self.path, 'channels.json', 'r'))

    
    def get_user_map(self):
        '''
        write a function to get a map between user id and user name
        '''
        userNamesById = {}
        userIdsByName = {}
        for user in self.users:
            userNamesById[user['id']] = user['name']
            userIdsByName[user['name']] = user['id']
        return userNamesById, userIdsByName        




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Export Slack history')

    
    parser.add_argument('--zip', help="Name of a zip file to import")
    args = parser.parse_args()
        

                        


        

        




            




        
        



