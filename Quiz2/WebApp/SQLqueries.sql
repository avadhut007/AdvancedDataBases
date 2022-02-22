Create table av_asg2 (
name varchar(255),
state varchar(255),
salary int, 
grade int, 
room int, 
telnum int, 
picture varchar(255), 
keywords varchar(255),
constraint pk_av_people primary key (name)
);


UPDATE av_people
SET picture = imagefile.filename WHERE name = username;