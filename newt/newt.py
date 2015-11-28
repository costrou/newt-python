from requests import Session
import os

NEWT_BASE_URL = "https://newt.nersc.gov/newt"
NEWT_MACHINES = ['hopper', 'carver', 'edison']
NEWT_SYSTEMS = ['hopper', 'carver', 'edison', 'pdsf', 'genepool', 'archive']

class NEWT:
    def __init__(self, username, password):
        self._session = Session()
        self.login(username, password)

    def login(self, username, password):
        """ Login user with password at NERSC for newt API

        Args:
           username: username for NERSC account
           password: password for NERSC account
        """
        url = NEWT_BASE_URL + '/login'
        resp = self._session.post(url, data={"username": username,
                                             "password": password})
        resp.raise_for_status()
        if resp.json()['auth'] and resp.json()['username'] == username:
            return resp.json()['auth']
        raise ValueError('Could not get authorized connection to NEWT!')

    def logout(self):
        """ Logout user """
        url = NEWT_BASE_URL + '/logout'
        resp = self._session.get(url)
        resp.raise_for_status()
        return not resp.json()['auth']

    @property
    def is_auth(self):
        """ Checks if user is authenticated """
        url = NEWT_BASE_URL + '/auth'
        resp = self._session.get(url)
        resp.raise_for_status()
        return resp.json()['auth']

    # Status API
    def status(self, system=None):
        """ Get status of system at NERSC

        Args:
           system: system at NERSC
        """
        url = NEWT_BASE_URL + '/status'
        if system:
            resp = self._session.get(url + "/" + system)
        else:
            resp = self._session.get(url)
        resp.raise_for_status()
        return resp.json()

    def motd(self):
        """ Get the message of the day at NERSC """
        url = NEWT_BASE_URL + '/status/motd'
        resp = self._session.get(url)
        resp.raise_for_status()
        return resp.text

    # File API
    def list(self, machine, remote_dir):
        """ List the contents of a directory at NERSC

        Args:
           machine: one of the available machines at NERSC
           remote_dir: path directory

        Output:
           List of inodes
               date: date modified
               user: inode user
               group: inode group
               hardlinks: TODO no clue
               name: inode name
               perms: permissions associated with inode
               size: inode file size
        """
        if machine not in NEWT_MACHINES:
            return ValueError('machine value must be specified')

        url = NEWT_BASE_URL + '/file/' + machine + remote_dir
        resp = self._session.get(url)
        resp.raise_for_status()
        return resp.json()

    def download(self, machine, remote_path, local_path=None):
        """ Download a file from NERSC

        Args:
           machine: one of the available machines at NERSC
           remote_dir: path to file to download
           local_path: path to save file
                       (default: local directory with remote_path filename)
        """
        if machine not in NEWT_MACHINES:
            return ValueError('machine value must be specified')

        if not local_path:
            local_path = remote_path.split('/')[-1]

        url = NEWT_BASE_URL + '/file/' + machine + remote_path
        resp = self._session.get(url, params={'view': 'read'}, stream=True)
        resp.raise_for_status()

        with open(local_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
        return local_path

    def upload(self, machine, remote_path, file_obj):
        """ Upload a file to NERSC

        Args:
           machine: one of the available machines at NERSC
           remote_dir: path to directory to store download
           file_obj: file() object to upload
        """
        if machine not in NEWT_MACHINES:
            return ValueError('machine value must be specified')

        remote_dir, remote_filename = os.path.split(remote_path)
        if remote_filename:
            files = {'file': (remote_filename, file_obj)}
        else:
            files = {'file': (file_obj.name, file_obj)}

        url = NEWT_BASE_URL + '/file/' + machine + remote_dir
        resp = self._session.post(url, files=files)
        resp.raise_for_status()
        return True

    # Command API
    def run_command(self, machine, command, loginenv=True):
        """Run command as username with or without shell environment. Note
        that running with the login environment takes additional
        time. Must be authorized.

        Args:
           machine: one of the available machines at NERSC
           command: commnad to run with arguments

        Output:
           error: stderr
           output: stdout
           status: OK | ERROR
        """
        if machine not in NEWT_MACHINES:
            return ValueError('machine value must be specified')

        url = NEWT_BASE_URL + '/command/' + machine
        resp = self._session.post(url, data={'executable': command, 'loginenv': loginenv})
        resp.raise_for_status() 
        return resp.json()
        
    # Queue API
    def queue_stat(self, machine, index=0, limit=10, **kwargs):
        """ Get the results of a qstat on a given machine at NERSC 

        Args:
            machine: one of the available machines at NERSC
            index: starting index of jobs in queue to list
            limit: number of jobs to show
            kwargs: additional keypairs to search (eg. queue=medium)
        """
        if machine not in NEWT_MACHINES:
            return ValueError('machine value must be specified')

        url = NEWT_BASE_URL + '/queue/' + machine
        params = {'index': index, 'limit': limit}
        params.update(kwargs)
        resp = self._session.get(url, params=params)
        resp.raise_for_status()
        return [Job(self._session, **job_info) for job_info in resp.json()]

    def queue_submit(self, machine, jobscript, jobfile=None):
        """Submit job to cluser (if jobfile is not None simply submit file on
        NERSC cluster

        Args:
           machine: one of the available machines at NERSC
           jobscript: String|file of submission file
           jobfile: remote path to jobfile to submit (will ignore jobscript is not none)

        Output:
           status: OK | ERROR
           error: submission error message
           jobid: job id of submitted job
        """
        if machine not in NEWT_MACHINES:
            return ValueError('machine value must be specified')

        if isinstance(jobscript, file):
            job = jobscript.read()
        else:
            job = jobscript

        if jobfile:
            data = {'jobscript': '', 'jobfile': jobfile}
        else:
            data = {'jobscript': job, 'jobfile': ''}

        url = NEWT_BASE_URL + '/queue/' + machine
        resp = self._session.post(url, data=data)
        resp.raise_for_status()
        return resp.json()


class Job:
    """ Represents a submitted job at NERSC

    Attr:
       jobid: id of the job
       name: name of job submitted into queue
       nodes: number of nodes requested by job
       procs: number of processors requested by job (procs = nodes * 24 usually)
       queue: queue job was submitted into
       rank: TODO not sure
       repo: TODO not sure
       state: Cancelled|Running|Complete
       status: shorthand for state
       submittime: time that job was submitted to queue
       timereq: time required for job to run
       timeuse: current time used by job
       user: username of user who submited job
    """
    def __init__(self, session, **kwargs):
        self._session = session
        self._data = kwargs

    def __getattr__(self, attr):
        return self._data[attr]

    def update(self):
        """ Give the current status of job """
        jobid = self.jobid.split('.')[0]
        machine = self.hostname
        
        url = NEWT_BASE_URL + '/queue/' + machine + "/" + jobid
        resp = self._session.get(url)
        resp.raise_for_status()
        
        job_info = resp.json()
        for key in job_info:
            setattr(self, key, job_info[key])
        return job_info

    def delete(self):
        """ delete job from queue at NERSC 

        Output:
            status: OK | ERROR
            output: stdout
            error: stderr
        """
        jobid = self.jobid.split('.')[0]
        machine = self.hostname

        url = NEWT_BASE_URL + '/queue/' + machine + "/" + jobid
        resp = self._session.delete(url)
        resp.raise_for_status()
        return resp.json()
