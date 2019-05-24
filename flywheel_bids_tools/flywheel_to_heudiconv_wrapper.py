import flywheel

class SeqInfo(object):

    def __init__(self, session_id):
        self.id = session_id
        self.fw = flywheel.Client()
        self.protocols = self.generate_sequences()

    def __len__(self):
        return len(self.protocols)

    def __iter__(self):
        for x in self.protocols:
            yield x

    def generate_sequences(self):

        session = self.fw.get(self.id)
        self.protocols = session.acquisitions()
