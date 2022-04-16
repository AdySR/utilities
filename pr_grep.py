>>> import os,sys,glob
>>> def py_grep(swt,pattern, filepathname):
	files = glob.iglob(filepathname)
	for file in files:
		for line in open(file):
			if pattern in line:
				if swt == '-l':
					print file
					break
				else:
					print line
					break

				
>>> py_grep('-l',"142515-142515-13|0710202015",r'C:\Actix_DI\repos\Output_7octMO_UE_Quantity\*.asv')
