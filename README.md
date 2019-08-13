# chego-firebase

This is a Google Firebase driver for Chego library.

## Install
```
npm install --save @chego/chego-firebase
```

## Usage
All you need to do to ensure that your queries are served by Google Firebase, simply create a new Chego object using the `chegoFirebase` and configuration object.

```
const { newChego } = require("@chego/chego");
const { chegoFirebase } = require("@chego/chego-firebase");
const chego = newChego(chegoFirebase, {
    apiKey: "xxxxxxxxxxxxxxxxxxxx",
    authDomain: "some-domain.firebaseapp.com",
    databaseURL: "https://some-domain.firebaseio.com",
    projectId: "some-domain",
    storageBucket: "some-domain.appspot.com",
    messagingSenderId: "3252523423"
});

await chego.connect();
const query = newQuery();

query.select('*').from('superheroes').where('origin').is.eq('Gotham City').limit(10);

chego.execute(query)
.then(result => { 
    console.log('RESULT:', JSON.stringify(result));
    chego.disconnect();
})
.catch(error => { 
    console.log('ERROR:', error); 
    chego.disconnect();
});


```
For more information on how `Chego` works with database drivers, please read [Chego Usage guide](https://github.com/chegojs/chego/blob/master/README.md).

All the info about the Google firebase you can find [here](https://firebase.google.com/).

## Features
#### custom conditions
If you want to check the complex condition for several object properties - without additional queries for object details, you can write a function and use it inside the `where` clause. This function must return a value that can be compared with additional conditions such as eq.

Check the example: We want to know which superhero is 100 km from Batcave. For this we need to know their current position and distance from the Batcave.
```
const getSuperheroDistanceFrom = (position) => (heroData) => {
     return ... math magic with position & heroData.position
}
query.select('*').from('superheroes').where(getSuperheroDistanceFrom({lat:42.762488, ,lng:-83.283120})).is.lt(100000);
```

## Contribute
There is still a lot to do, so if you want to be part of the Chego project and make it better, it's great.
Whether you find a bug or have a feature request, please contact us. With your help, we'll make it a great tool.

[How to contribute](https://github.com/orgs/chegojs/chego/CONTRIBUTING.md)

Follow our kanban boards to be up to date

[Kanban boards](https://github.com/orgs/chegojs/projects/2)

Join the team, feel free to catch any task or suggest a new one.

## License

Copyright (c) 2019 [Chego Team](https://github.com/orgs/chegojs/people)

Licensed under the [MIT license](LICENSE).