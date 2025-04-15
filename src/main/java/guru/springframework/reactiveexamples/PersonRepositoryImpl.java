package guru.springframework.reactiveexamples;

import guru.springframework.reactiveexamples.domain.Person;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Created by jt on 2/27/21.
 */
public class PersonRepositoryImpl implements PersonRepository {

    Person mike = new Person(1,"Mike","Doe");
    Person sanaz = new Person(2,"Sanaz","Bon");

    @Override
    public Mono<Person> getById(Integer id) {
        Flux<Person> personFlux = findAll();
        Mono<Person> personMono = personFlux.filter(person -> person.getId() == id).next();
        return Mono.justOrEmpty(personMono.block());
        //return findAll().filter(person -> person.getId() == id).next();
    }

    @Override
    public Flux<Person> findAll() {
        return Flux.just(mike,sanaz);
    }
}
