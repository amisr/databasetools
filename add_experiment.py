#!/opt/websites/database/calendar/venv/bin/python

###########################################################
#
#    Title: add_experiment.py
#
#    Description: This script adds an experiment from the
#                 AMISR_PROCESSED NAS into the SRI calendar
#                 database. It requires a Madrigal.ini file
#                 to do this.
#
#    Author: M. Nicolls 2007
#
#    Modified: 7 Oct 2019 - Ashton Reimer
#              Moved scripts from io to io2, upgraded to
#              python2.7
#
#    Modified: 5 Oct 2022 - Pablo Reyes
#              Change classification from:
#              Alternating Code -> E-region bottom F-region
#              Long Pulse -> F-region
#              adds the integration time to the left column
#              avoids duplications of files. After all the uncorrected Ne
#              is included in the regular file
#              - avoids duplication of images, e.g. Ne and Nenotr
#              - Sorts the lists in order to have the links organized
#              - Fixed Vvels different integration times
#    Modified: 6 Oct 2022 - Pablo Reyes
#              - Adding a prefix to the figure name to allow
#                multiple integration times to coexist.
#    Modified: 24 Oct 2022 - Pablo Reyes
#              - Fixed a bug in line ~ 244 image_label
#                This bug was creating problems in adding processed data files
#    Modified: 06 Dec 2022 - Pablo Reyes
#              - code put in place to avoidthe copy of the same file twice one
#                for NeNoTr and the other for fitted data, since they are the
#                same file
#    Modified: 06 Mar 2023 - Pablo Reyes
#              - Eliminating description "bottom..." it was too verbose.
#    Modified: 06 Apr 2023 - Pablo Reyes
#              - adding space in the fig names after the h5 file prefix.
#    Modified: 16 Apr 2023 - Pablo Reyes
#              - fixing a bug in uploading vvels figures
###########################################################



import sys
import os.path
import tables
import scipy
import scipy.io
import glob
import shutil
import copy
import datetime
import ConfigParser
import shutil
import re

import logging
LOG_DIR = '/opt/websites/database/calendar/amisrdb/ExperimentDetails/logs'
LOG_FILENAME = 'GenExpLists.log'


#############################

copyON = 1
linkMad = 1
#MadPath = '%(MADRIGAL_HTTP_PATH)s'
MadPath = '%(MADRIGAL3_HTTP_PATH)s'

def usage():
    print "usage: ", sys.argv[0]
    print "\t EXPERIMENT DIRECTORIES"

    sys.exit(2)

def get_intg_time(x):
    mpos = x.find("minute")
    spos = x.find("second")
    if mpos>=0:
        subx = x[(mpos-7):mpos]
        secs = 60*int(subx[subx.find(" "):])
    elif spos>=0:
        subx = x[(spos-7):spos]
        secs = int(subx[subx.find(" "):])
    else:
        secs = 1e99
    return secs

if __name__ == '__main__':

    FILE_OUT = 'Data.ini'    

    SEC_TEMPLATE = {'Path':'','Images':{'Count':0,'Hash':[]}, 'Links':{'Count':0}}

    INSTRUMENTS = {
        '61': {
            'OUTPUT_PATH': '/opt/websites/database/calendar/amisrdb/ExperimentDetails/PFISR/Experiments',
            'INST_MNEUMONIC': 'pfa'
        },
        '91': {
            'OUTPUT_PATH': '/opt/websites/database/calendar/amisrdb/ExperimentDetails/RISR-N/Experiments',
            'INST_MNEUMONIC': 'ran'
        },
    }

    list_of_datafiles = []
    avoid_overwrite = True
    avoid_overwrite = False


    try:
        dirs2do = sorted(glob.glob(sys.argv[1]))
    except:
        usage()  
        
    for direc in dirs2do:
        
        if not os.path.isdir(direc) or os.path.islink(direc):
            continue
        
        print 'Doing %s' % direc
        
        try:
            MAD_FILE = os.path.join(direc,'Madrigal.ini')
            MAD_PATH = os.path.dirname(MAD_FILE)
            MAD_EXP = os.path.basename(MAD_PATH)
      
            # read config file
            config = ConfigParser.ConfigParser()
            config.optionxform = str
            config.read(MAD_FILE)    
        except:
            raise IOError, 'Error reading Madrigal ini file'
    
        # get instrument
        try:
            inst=config.get('Experiment','instrument')
            expName = config.get('DEFAULT','ExperimentName')
            OUTPUT_PATH = os.path.join(INSTRUMENTS[inst]['OUTPUT_PATH'],expName)            
        except:
            raise IOError, 'Could not find instrument'
        
        # 
        if MAD_EXP != expName:
            config.set('DEFAULT','ExperimentName',MAD_EXP)
        
        DSTR_OUT={}
        if copyON:
            DSTR_OUT['Data Files']=copy.deepcopy(SEC_TEMPLATE)
            DSTR_OUT['Data Files']['Path']='DataFiles'
            fdir = os.path.join(OUTPUT_PATH,DSTR_OUT['Data Files']['Path'])
            if not os.path.exists(fdir):
                try: 
                    os.mkdir(fdir)
                except:
                    raise IOError, 'Unable to make dir %s' % fdir

            DSTR_OUT['Additional Plots']=copy.deepcopy(SEC_TEMPLATE)
            DSTR_OUT['Additional Plots']['Path']='AdditPlots'
            additdir = os.path.join(OUTPUT_PATH,DSTR_OUT['Additional Plots']['Path'])
            if not os.path.exists(additdir):
                try: 
                    os.mkdir(additdir)
                except:
                    raise IOError, 'Unable to make dir %s' % additdir

        uploaded_files = []
        uploaded_figs = []
        for ifile in range(1,100000):
            tsec = 'File%s' % ifile

            if not config.has_section(tsec):
                break;
                
            try:
                fname = config.get(tsec,'hdf5Filename')
                type = config.get(tsec,'type')
                ckindat = config.get(tsec,'ckindat')
                kindat = config.get(tsec,'kindat')
                status = config.get(tsec,'status')
                category = int(config.get(tsec,'category'))
                history = config.get(tsec,'history')
            except:
                raise IOError, 'Could not read sec %s' % tsec

            print(ckindat)
            tname=ckindat
            classification_style = "Ionospheric_Regions" # "Pulse_Type"
            if classification_style == "Ionospheric_Regions":
                if tname.find('Barker')>=0 or tname.find("D-region")>=0:
                    #classname = "D-region bottom E-region (Barker/mc)"
                    classname = "D-region (Barker/mc)"
                elif tname.find("Alternating Code")>=0 or \
                        tname.find("E-region")>=0:
                    #classname = 'E-region bottom F-region (Alternating Codes)'
                    classname = 'E-region (Alternating Codes)'
                elif tname.find('Long Pulse')>=0 or \
                        (tname.find("F-region")>=0 and
                                tname.find("E-region")<0):
                    classname = 'F-region (Long Pulse)'
            elif classification_style == "Pulse_Type":
                if tname.startswith('Alternating Code'):
                    classname = 'Alternating Code'
                elif tname.startswith('Long Pulse'):
                    classname = 'Long Pulse'

            if tname.find("Velocity")>=0 or tname.find("Vector")>=0 \
                    or tname.find("Vel.") >=0 :
                classname = "Resolved Velocity"

            tname = classname

            if DSTR_OUT.has_key(tname):
                tpath = DSTR_OUT[tname]['Path']
            else:
                DSTR_OUT[tname]=copy.deepcopy(SEC_TEMPLATE)
                tpath = 'Path%s' % str(len(DSTR_OUT.keys()))
                DSTR_OUT[tname]['Path'] = tpath

            # make dir
            tdir = os.path.join(OUTPUT_PATH,tpath)
            if not os.path.exists(tdir):
                try: 
                    os.mkdir(tdir)
                except:
                    raise IOError, 'Unable to make dir %s' % tdir
            
            # data file        
            if copyON:
                try:
                    print("Copy fname:",fname)
                    target = os.path.join(fdir,os.path.basename(fname))
                    print("Copy to :", target)
                    if avoid_overwrite and os.path.exists(target):
                        print(target,"already exists. skipping")
                    else:
                        if target not in list_of_datafiles:
                            # This avoids to copy the same file twice for NeNoTr and for fitted data
                            # since both are the same file
                            list_of_datafiles.append(target)
                            shutil.copyfile(fname, target)
                        else:
                            print(target,"was already copied")
                except Exception, e:
                    raise Exception,'Exception: Unable to copy file %s\n%s' % (fname,str(e))
            
            if copyON:
                #tit = ckindat + ' - hdf'
                if category != 1:
                    tit = classname + ', status:' + status  + ', history:' + history + ' - hdf'
                else:
                    tit = classname + ' - hdf'
                if not DSTR_OUT['Data Files']['Images'].has_key(tit):
                    DSTR_OUT['Data Files']['Images']['Count']+=1
                    DSTR_OUT['Data Files']['Images'][tit] = {'Count':0,
                            'imgCount':DSTR_OUT['Data Files']['Images']['Count']}
                    DSTR_OUT['Data Files']['Images']['Hash'].append(tit)
                file2upload = os.path.basename(fname)
                if file2upload not in uploaded_files:
                    uploaded_files.append(file2upload)
                    DSTR_OUT['Data Files']['Images'][tit]['Count']+=1
                    image_label = 'image%s%s' % (
                            # Fixed on Oct 24, 2022 by Pablo Reyes 
                            #str(DSTR_OUT['Data Files']['Images']['Count']),
                            str(DSTR_OUT['Data Files']['Images'][tit]['imgCount']),
                            chr(DSTR_OUT['Data Files']['Images'][tit]['Count']-1+ord('a')))
                    DSTR_OUT['Data Files']['Images'][tit][image_label] = file2upload
                                                                    
            # images
            for iimg in range(1,100000):
                timg = 'image%s' % iimg
                timgtit = 'imageTitle%s' % iimg
                if not config.has_option(tsec,timg):
                    break;            
                img = config.get(tsec,timg)
                tit = config.get(tsec,timgtit)
                
                #if tit.__contains__('Electron Density'):
                #    tit = 'Electron Density'
                
                if tit.__contains__('Geometry Plot'):
                    try:
                        # Nov 15, 2022. P. Reyes
                        # Adding a prefix to allow different geo plots like risrn 20200229.001 GPSN.v01.85.db92.risrn
                        fig_prefix = tit[:tit.find(' ')]
                        fig2upload = fig_prefix + " " +  os.path.basename(img)
                        print("Copy img:",img)
                        #print("Copy to :",os.path.join(additdir,os.path.basename(img)))
                        print("Copy to :",os.path.join(additdir,fig2upload))
                        shutil.copyfile(img,os.path.join(additdir,fig2upload))
                    except:
                        raise IOError, 'Unable to copy file %s' % img
                    tname_geo='Additional Plots' # new thumbnail
                    if not DSTR_OUT[tname_geo]['Images'].has_key(tit):
                        DSTR_OUT[tname_geo]['Images']['Count']+=1
                        DSTR_OUT[tname_geo]['Images'][tit] = {'Count':0,'imgCount':DSTR_OUT[tname_geo]['Images']['Count']}
                        DSTR_OUT[tname_geo]['Images']['Hash'].append(tit)
                    DSTR_OUT[tname_geo]['Images'][tit]['Count']+=1
                    DSTR_OUT[tname_geo]['Images'][tit]['image%s%s' % (
                        str(DSTR_OUT[tname_geo]['Images']['Count']),
                        chr(DSTR_OUT[tname_geo]['Images'][tit]['Count']-1+ord('a')))] = fig2upload
                    continue

                if type == 'velocity':
                    try:    
                        p1 = re.compile('\d+sec')
                        sintgsec = p1.findall(tit)[0]
                        tit = tit[tit.find(sintgsec):]
                        #tit = sintgsec + ' Magnitude and Direction'
                    except Exception as e:
                        print('Problem Encounterd: Skipping.\n%s' % str(e))
                        continue
                    if os.path.basename(img).find(sintgsec) < 0:
                        # intgseconds doesn't match the image file
                        continue
                try:
                    if type != 'velocity':
                        fig_prefix = tit[:tit.find(' ')]
                        fig2upload = fig_prefix + " " + os.path.basename(img)
                    else:
                        fig2upload = os.path.basename(img)
                    img_destiny = os.path.join(tdir, fig2upload)
                    #print('tit:',tit,"copy",img)
                    shutil.copyfile(img,img_destiny)
                except:
                    raise IOError, 'Unable to copy file %s' % img
                            
                if not DSTR_OUT[tname]['Images'].has_key(tit):
                    DSTR_OUT[tname]['Images']['Count']+=1
                    DSTR_OUT[tname]['Images'][tit] = {'Count':0,'imgCount':DSTR_OUT[tname]['Images']['Count']}
                    DSTR_OUT[tname]['Images']['Hash'].append(tit)
                unique_name = tname+'|fig2upload:'+fig2upload
                if unique_name not in uploaded_figs:
                    uploaded_figs.append(unique_name)
                    print(unique_name)
                    DSTR_OUT[tname]['Images'][tit]['Count']+=1
                    DSTR_OUT[tname]['Images'][tit]['image%s%s' % (
                        str(DSTR_OUT[tname]['Images']['Count']),
                        chr(DSTR_OUT[tname]['Images'][tit]['Count']-1+ord('a')))] = fig2upload

        with open(os.path.join(OUTPUT_PATH,FILE_OUT),'w') as fid:
            #for key in sorted(DSTR_OUT.keys(), key=lambda x:(x[0:2],get_intg_time(x))):
            for key in sorted(DSTR_OUT.keys(), key=lambda x:(x[0],len(x))):
                fid.write('[%s]\n\n' % key)
                fid.write('Path: %s\n\n' % DSTR_OUT[key]['Path'])
                for skey in  sorted(DSTR_OUT[key]['Images']['Hash']):
                    fid.write('imageTitle%d: %s\n' % (DSTR_OUT[key]['Images'][skey]['imgCount'],skey))
                    for i in range(DSTR_OUT[key]['Images'][skey]['Count']):
                        try:
                            bp = 'image%d%s' % (DSTR_OUT[key]['Images'][skey]['imgCount'],chr(i+ord('a')))
                            towrite = '%s: %s\n' % (bp, DSTR_OUT[key]['Images'][skey][bp])
                            fid.write(towrite)
                        except:
                            print '???'
                            print 'skey', skey
                            print 'bp',bp
                            print 'to write', towrite
                    fid.write('\n')

            if linkMad:
                expId = MAD_EXP
                year = int(expId[0:4])
                month = int(expId[4:6])
                day = int(expId[6:8])
                num = int(expId[10:12])
                #extChar =chr(num+ord('a')-1)
                if num <= 26:
                    extChar = chr(96 + num)
                else:
                    extChar = chr(96 + (num-1)//26) + chr(97 + (num-1)%26)
                thisDate = datetime.datetime(year, month, day)
                #madLink='%sexp=%s/%s/%s%s&displayLevel=0' % (MadPath,thisDate.strftime('%Y').lower(),INSTRUMENTS[inst]['INST_MNEUMONIC'],thisDate.strftime('%d%b%y').lower(), extChar)
                madLink='%sexperiment_list=/%s/%s/%s%s' % (MadPath,thisDate.strftime('%Y').lower(),INSTRUMENTS[inst]['INST_MNEUMONIC'],thisDate.strftime('%d%b%y').lower(), extChar)
                # Madrigal2:
                # http://isr.sri.com/madrigal/cgi-bin/madExperiment.cgi?exp=2023/pfa/01mar23b&displayLevel=0
                # In Madrigal3:
                # https://data.amisr.com/madrigal/showExperiment?experiment_list=/2023/pfa/01mar23b
                fid.write('[Links]\n\n')
                fid.write('Path: %s\n\n' % '')
                fid.write('imageTitle1: %s\n' % ('Access Data from Madrigal'))            
                bp = 'image1a'
                fid.write('%s: %s\n\n' % (bp, madLink))              
            
