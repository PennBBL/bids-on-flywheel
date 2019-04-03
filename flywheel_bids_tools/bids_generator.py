from flywheel_bids_tools.query_bids import process_acquisition
from flywheel_bids_tools.utils import is_nan, unlist_item
import pandas as pd
import ast

class BidsGenerator:

    def __init__(self):
        self.dictionary = None
        self.valid = None
        self.subject = None
        self.session = None
        self.acq = None
        self.ce = None
        self.rec = None
        self.run = None
        self.mod = None
        self.task = None
        self.dir = None
        self.intention = None
        self.name = None
        self.suffix = None
        self.shim = None
        self.id = None


    def parse_row(self, pd_row):

        self.dictionary = pd_row.to_dict()
        self.id = self.dictionary['acquisition.id']
        self.name = self.dictionary['name']
        self.subject = self.dictionary['subject.label']
        self.session = self.dictionary['session.label']
        self.acq = self.dictionary['info_BIDS_Acq']
        self.ce = self.dictionary['info_BIDS_Ce']
        self.rec = self.dictionary['info_BIDS_Rec']
        self.run = self.dictionary['info_BIDS_Run']
        self.mod = self.dictionary['info_BIDS_Mod']
        self.task = self.dictionary['info_BIDS_Task']
        self.dir = self.dictionary['info_BIDS_Dir']
        self.intention = [x['Folder'] for x in ast.literal_eval(self.dictionary['info_BIDS_IntendedFor'])]
        self.bids_name = self.dictionary['info_BIDS_Filename']

    def find_suffix(self):

        if "name" not in self.dictionary.keys():
            return None

    def build_intention_path(self, row):

        path = "ses-{0}/{1}/{2}".format(
            row['session.label'], row['info_BIDS_Folder'], row['info_BIDS_Filename'])
        return path

    def update_intention(self, fw):

        if is_nan(self.intention) or self.intention == '':
            return None
        else:
            acq = fw.get(self.id)
            acq_df = process_acqdfuisition(acq_id=acq.id, client=fw, target_cols=["info_ShimSetting"])
            if "info_ShimSetting" not in acq_df.columns:
                raise AssertionError("No shim settings found for this file!")
            self.shim = tuple(acq_df.dropna().info_ShimSetting.values[0])
            other_acquisitions = []
            for folder in self.intention:
                session = fw.get(acq.parents['session'])
                for ac in session.acquisitions():
                    #if ac.id == self.id:
                    #    continue
                    temp = process_acquisition(ac.id, fw, target_cols=["info_ShimSetting", "info_BIDS_Filename", "info_SeriesDescription", "info_BIDS_Folder"])
                    temp['session.label'] = session.label
                    other_acquisitions.append(temp)


            other_acquisitions = pd.concat(other_acquisitions, sort=False, ignore_index=True)
            other_acquisitions = other_acquisitions.dropna(subset=['info_ShimSetting'])
            other_acquisitions['info_ShimSetting'] = other_acquisitions['info_ShimSetting'].map(tuple)
            intendedfor = other_acquisitions.loc[ (other_acquisitions['info_ShimSetting'] == self.shim) & (other_acquisitions['info_BIDS_Folder'].isin(self.intention)), ].reset_index()
            print(self.subject, self.session, self.name)
            #print(intendedfor)
            result = intendedfor.apply(self.build_intention_path, axis=1)
            print(result)
            #if len(result == 0):
        #        return None
            target_file = [f for f in acq.files if f.name == self.name][0]
            print(target_file['info']['IntendedFor'])
            #update = acq.update_file_info(target_file.name, {'IntendedFor': list(result.values)})
            #return result #update
