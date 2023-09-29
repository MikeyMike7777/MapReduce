# Michael Mathews
# Assignment 9
import sys
from mrjob.step import MRStep
from mrjob.job import MRJob

class MRPitching(MRJob):
    def configure_args(self):
        super(MRPitching, self).configure_args()
        self.add_passthru_arg(
            '--limit', default='100',
            help="Specify the minimum occurrence for output")

    def mapper(self, _, line):
        if line.startswith("playerID,"):
            return
        data = line.split(',')
        playerid, yearid, p_W, p_IPOUTS, p_H, p_BB  = data[0], int(data[1]), int(data[5]), int(data[12]), int(data[13]), int(data[16])
        if yearid > 1900:
            yield playerid, [p_W, p_IPOUTS, p_H, p_BB]

    # Reducer function to sum up values by key and calculate whip
    def reducer(self, key, values):
        total_p_H, total_p_BB, total_p_IPOUTS, total_p_W = 0, 0, 0, 0
        for value in values:
            total_p_W += value[0]
            total_p_IPOUTS += value[1]
            total_p_H += value[2]
            total_p_BB += value[3]
        if total_p_W >= 300:
            whip = 3*(total_p_H+total_p_BB)/total_p_IPOUTS
            yield key, whip

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                reducer=self.reducer)
        ]

if __name__ == '__main__':
    MRPitching.run()
